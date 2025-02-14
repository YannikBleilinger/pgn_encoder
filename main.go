package main

import (
	"bufio"
	"database/sql"
	json2 "encoding/json"
	"errors"
	"fmt"
	"github.com/corentings/chess/v2"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
)

/**
	Moves can be between 0 and 4287 where
	0-87 (8*4*3) promotion squares for black (8 start squares, 4 promotion pieces, 3 end relative end squares)
	88-4183 (64*64) all moves between squares
	4184-4271 (8*4*3) promotions squares for white
**/

type positionStruct struct {
	fen  string // fen string
	move int16  // 0 - 4160
	elo  int16
}

const numMoves = 4272

//TIP <p>To run your code, right-click the code and select <b>Run</b>.</p> <p>Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.</p>

func check(e error) {
	if e != nil {
		fmt.Printf("ERROR: %v\n", e)
	}
}

func main() {
	testAllMoves()
	return
	dataPath := "D:\\ChessGPT Data\\Okt_2024\\data.pgn"
	//dataPath := "/home/ybleilinger/chessGPT/chessVenv/data/temp/lichess_db_standard_rated_2023-08.pgn"
	//outPath := ".\\files\\encoded_positions.bin"
	runtime.GOMAXPROCS(runtime.NumCPU())
	numWorkers := runtime.NumCPU()

	cpuF, err := os.Create("cpu.prof")
	check(err)
	memF, err := os.Create("mem.prof")
	check(err)
	defer cpuF.Close()
	defer memF.Close()
	check(err)
	err = pprof.StartCPUProfile(cpuF)
	check(err)
	defer pprof.StopCPUProfile()

	f, err := os.Open(dataPath)
	check(err)
	defer f.Close()

	// database
	db, err := sql.Open("sqlite3", ".\\files\\chess.db")
	check(err)
	defer db.Close()

	createTableSQL := `
		CREATE TABLE IF NOT EXISTS positions (
		    id INTEGER PRIMARY KEY,
		    fen TEXT NOT NULL,
		    move INTEGER CHECK(move BETWEEN 0 AND 4272),
		    elo INTEGER NOT NULL,
		    UNIQUE(fen, move)
		)`

	/*
	   	createTableSQL := `CREATE TABLE IF NOT EXISTS positions (
	       fen TEXT PRIMARY KEY,
	       move_dist TEXT
	   )`

	*/

	_, err = db.Exec(createTableSQL)
	check(err)

	scanner := bufio.NewScanner(f)
	gameChan := make(chan string, 100_000)
	posChan := make(chan positionStruct, 100_000)
	var wgWrite, wgParse sync.WaitGroup

	wgWrite.Add(1)
	go databaseWriter(posChan, db, &wgWrite)
	//go fileWriter(&wgWrite, fileCmdChan, outPath)
	for i := 0; i < numWorkers; i++ {
		wgParse.Add(1)
		//go pgnToFen(gameChan, &wgParse, &seen, fileCmdChan)
		go pgnToFen(gameChan, &wgParse, posChan)
	}
	totalGames := 1_000_000
	currentGames := 0
	fmt.Println("Starting to process games")
	var chunk strings.Builder
	emptyLines := 0
	for scanner.Scan() {
		line := scanner.Text()
		// If line is empty, increase the empty line counter
		if strings.TrimSpace(line) == "" {
			chunk.WriteString("\n")
			emptyLines++
			if emptyLines == 2 {
				// Send the accumulated game to the channel
				gameChan <- chunk.String()
				currentGames++
				chunk.Reset()  // Reset chunk for the next game
				emptyLines = 0 // Reset empty line counter

				// Print progress
				if currentGames%20_000 == 0 {
					fmt.Printf("Games processed: %v\n", currentGames)
				}

				// Stop if totalGames limit is reached

				if currentGames == totalGames {
					break
				}

				emptyLines = 0
			}
			continue // Skip appending empty lines to the chunk
		}
		chunk.WriteString(line + "\n")
	}
	if chunk.Len() > 0 {
		gameChan <- chunk.String()
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Scan failed: %v", err)
	}
	close(gameChan)
	wgParse.Wait()
	close(posChan)
	wgWrite.Wait()

	err = pprof.WriteHeapProfile(memF)

	var count int
	db.QueryRow("SELECT COUNT(*) FROM positions").Scan(&count)
	fmt.Printf("Result: %v\n", count)
}

func pgnToFen(c <-chan string, wg *sync.WaitGroup, posChan chan positionStruct) {
	defer wg.Done()
	var posPool = sync.Pool{
		New: func() interface{} {
			return new(positionStruct)
		},
	}
	for pgnString := range c {

		pgn, err := chess.PGN(strings.NewReader(pgnString))
		if err != nil {
			fmt.Printf("Error parsing pgn: %v\n", err)
			continue
		}
		game := chess.NewGame(pgn)
		moves := game.Moves()
		for i, position := range game.Positions() {
			// skip last "checkmate" board state
			if i == len(game.Positions())-1 {
				break
			}
			move := moves[i]
			moveIdx := getIdxFromMove(move)
			// get elo from player on turn
			var currentElo int16
			if position.Turn() == chess.White {
				elo, err := strconv.ParseInt(game.GetTagPair("WhiteElo"), 10, 64)
				currentElo = int16(elo)
				check(err)
			} else {
				elo, err := strconv.ParseInt(game.GetTagPair("BlackElo"), 10, 64)
				currentElo = int16(elo)
				check(err)
			}
			pos := posPool.Get().(*positionStruct)
			pos.fen = position.String()
			pos.move = moveIdx
			pos.elo = currentElo
			posChan <- *pos
			posPool.Put(pos)
			//posChan <- positionStruct{position.String(), moveIdx, currentElo}
		}
	}
}

func newDatabaseWriter(positionChan <-chan positionStruct, db *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()
	_, err := db.Exec("PRAGMA synchronous = OFF; PRAGMA journal_mode = MEMORY; PRAGMA temp_store = MEMORY;")
	check(err)

	tx, err := db.Begin()
	check(err)
	selectStmt, err := tx.Prepare("SELECT move_dist FROM positions WHERE fen = ?")
	check(err)
	defer selectStmt.Close()

	updateStmt, err := tx.Prepare("UPDATE positions SET move_dist = ? WHERE fen = ?")
	check(err)
	defer updateStmt.Close()

	insertStmt, err := tx.Prepare("INSERT INTO positions (fen, move_dist) VALUES (?, ?)")
	check(err)
	defer insertStmt.Close()

	count := 0
	batchSize := 100_000
	for pos := range positionChan {
		var json string
		var freq []int
		exists := true
		// weight move count by players elo
		var weight int
		switch {
		case pos.elo <= 1000:
			weight = 1
		case pos.elo <= 1500:
			weight = 2
		case pos.elo <= 2000:
			weight = 4
		case pos.elo <= 2300:
			weight = 5
		case pos.elo <= 2600:
			weight = 10
		case pos.elo <= 3000:
			weight = 15
		default:
			weight = 30
		}

		// get existing record
		err = selectStmt.QueryRow(pos.fen).Scan(&json)
		if errors.Is(err, sql.ErrNoRows) {
			exists = false
			freq = make([]int, numMoves)
		} else if err != nil {
			fmt.Printf("Error retrieving rows: %v", err)
			continue
		} else {
			err = json2.Unmarshal([]byte(json), &freq)
			if err != nil {
				fmt.Printf("Error parsing json: %v", err)
				continue
			}
		}

		freq[pos.move] += weight

		newJson, err := json2.Marshal(freq)
		check(err)
		if exists {
			_, err = updateStmt.Exec(newJson, pos.fen)
			check(err)
		} else {
			_, err = insertStmt.Exec(pos.fen, newJson)
			check(err)
		}

		count++
		if count%batchSize == 0 {
			err = tx.Commit()
			check(err)
			tx, err = db.Begin()
			check(err)
			selectStmt, err = tx.Prepare("SELECT move_dist FROM positions WHERE fen = ?")
			check(err)
			updateStmt, err = tx.Prepare("UPDATE positions SET move_dist = ? WHERE fen = ?")
			check(err)
			insertStmt, err = tx.Prepare("INSERT INTO positions (fen, move_dist) VALUES (?, ?)")
			check(err)
			var storedPos int
			db.QueryRow("SELECT COUNT(*) FROM positions").Scan(&storedPos)
			fmt.Printf("Stored positions in db: %v\n", storedPos)
		}
	}
	err = tx.Commit()
	check(err)
}

func databaseWriter(positionChan <-chan positionStruct, db *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()

	_, err := db.Exec("PRAGMA synchronous = OFF; PRAGMA journal_mode = MEMORY; PRAGMA temp_store = MEMORY;")

	tx, err := db.Begin()
	check(err)

	check(err)
	var stmt *sql.Stmt
	defer stmt.Close()
	processedPositions := 0
	for pos := range positionChan {
		if processedPositions%1_000_000 == 0 {
			err = tx.Commit()
			check(err)
			tx, err = db.Begin()
			check(err)
			stmt, err = tx.Prepare(`
				Insert INTO positions (fen, move, elo)
				Values (?, ?, ?)
				ON CONFLICT(fen, move) DO UPDATE SET
					elo = excluded.elo
				WHERE excluded.elo > elo
			`)
			check(err)
			var count int
			db.QueryRow("SELECT COUNT(*) FROM positions").Scan(&count)
			fmt.Printf("Stored positions in db: %v\n", count)
		}
		if _, err := stmt.Exec(pos.fen, pos.move, pos.elo); err != nil {
			tx.Rollback()
			log.Printf("exec error: %v\n", err)
		} else {
			processedPositions++
		}

	}
	err = tx.Commit()
	check(err)
}

func getIdxFromMove(move *chess.Move) int16 {
	var moveIdx int16 = -1
	if move.Promo() != chess.NoPieceType {
		moveIdx = getPromotionMoveIdx(move)
	} else {
		moveIdx = int16(move.S1())*64 + int16(move.S2()) + 88
	}
	return moveIdx
}

func getPromotionMoveIdx(move *chess.Move) int16 {
	var moveIdx int16 = -1
	startFile := int16(move.S1().File())
	endFile := int16(move.S2().File())
	diff := endFile - startFile + 1
	moveIdx = startFile*12 + diff*4

	// if white add to end of all move idx
	if move.S1().Rank() == chess.Rank7 {
		moveIdx += 64*64 + 88
	}
	// get promotion piece value
	switch move.Promo() {
	case chess.Queen:
		moveIdx += 0
	case chess.Rook:
		moveIdx += 1
	case chess.Bishop:
		moveIdx += 2
	case chess.Knight:
		moveIdx += 3
	default:
		panic("Promotion piece is fucked")
	}
	return moveIdx - 4
}

func testPromotionIndex(color chess.Color) {
	var startRank chess.Rank
	var endRank chess.Rank
	if color == chess.White {
		startRank = chess.Rank7
		endRank = chess.Rank8
	} else {
		startRank = chess.Rank2
		endRank = chess.Rank1
	}
	startFiles := []chess.File{chess.FileA, chess.FileB, chess.FileC, chess.FileD, chess.FileE, chess.FileF, chess.FileG, chess.FileH}

	endFiles := make([][]chess.File, 8)
	endFiles[0] = []chess.File{chess.FileA, chess.FileB}
	endFiles[1] = []chess.File{chess.FileA, chess.FileB, chess.FileC}
	endFiles[2] = []chess.File{chess.FileB, chess.FileC, chess.FileD}
	endFiles[3] = []chess.File{chess.FileC, chess.FileD, chess.FileE}
	endFiles[4] = []chess.File{chess.FileD, chess.FileE, chess.FileF}
	endFiles[5] = []chess.File{chess.FileE, chess.FileF, chess.FileG}
	endFiles[6] = []chess.File{chess.FileF, chess.FileG, chess.FileH}
	endFiles[7] = []chess.File{chess.FileG, chess.FileH}

	promPieces := []chess.PieceType{chess.Queen, chess.Rook, chess.Bishop, chess.Knight}

	for i := 0; i < 8; i++ {
		for j := 0; j < 3; j++ {
			for _, p := range promPieces {
				if (i == 0 && j == 2) || (i == 7 && j == 2) {
					continue
				}
				startSquare := chess.NewSquare(startFiles[i], startRank)
				endSquare := chess.NewSquare(endFiles[i][j], endRank)
				move := chess.Move{}
				move.SetStartSquare(startSquare)
				move.SetEndSquare(endSquare)
				move.SetPromoPiece(p)

				fmt.Printf("Index: %v, Move: %v\n", getPromotionMoveIdx(&move), move.String())
			}
		}
	}
}

func testAllMoves() {
	testPromotionIndex(chess.Black)
	var startSquare chess.Square
	var endSquare chess.Square
	index := 88
	for sR := chess.Rank1; sR <= chess.Rank8; sR++ {
		for sF := chess.FileA; sF <= chess.FileH; sF++ {
			startSquare = chess.NewSquare(sF, sR)
			for eR := chess.Rank1; eR <= chess.Rank8; eR++ {
				for eF := chess.FileA; eF <= chess.FileH; eF++ {
					endSquare = chess.NewSquare(eF, eR)
					move := chess.Move{}
					move.SetStartSquare(startSquare)
					move.SetEndSquare(endSquare)
					move.SetPromoPiece(chess.NoPieceType)

					fmt.Printf("Idx: %v, Move: %v\n", getIdxFromMove(&move), move.String())
					index++
				}
			}
		}
	}
	testPromotionIndex(chess.White)
}
