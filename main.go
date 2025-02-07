package main

import (
	"bufio"
	"database/sql"
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

// todo: add repetitions integer in global vector
// hash map if pos1 == pos2 replace if elo1 < elo2
// compare +1 for same piece + color indicator or +1/-1 one hot vector

/**
	Moves can be between 0 and 4287 where
	0-87 (8*4*3) promotion squares for black (8 start squares, 4 promotion pieces, 3 end relative end squares)
	88-4184 (64*64) all moves between squares
	4184-4271 (8*4*3) promotions squares for white
**/

type posRecord struct {
	elo   int
	index int64
}

type fileCmd struct {
	cmdType string // append or update
	data    []byte
	offset  int64      // used for update
	result  chan int64 // used for append to return offset
}

type positionStruct struct {
	fen  string // fen string
	move int16  // 0 - 4160
	elo  int16
}

const recordSize = 65*7 + 2

//TIP <p>To run your code, right-click the code and select <b>Run</b>.</p> <p>Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.</p>

func check(e error) {
	if e != nil {
		fmt.Printf("ERROR: %v\n", e)
	}
}

func main() {

	dataPath := "D:\\ChessGPT Data\\Okt_2024\\data.pgn"
	//dataPath := "/home/ybleilinger/chessGPT/chessVenv/data/temp/lichess_db_standard_rated_2023-08.pgn"
	//outPath := ".\\files\\encoded_positions.bin"
	numWorkers := runtime.NumCPU() - 1

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
	    fen TEXT PRIMARY KEY,
	    move INTEGER CHECK(move BETWEEN 0 AND 4287),
	    elo INTEGER
	)`

	_, err = db.Exec(createTableSQL)
	check(err)

	scanner := bufio.NewScanner(f)
	gameChan := make(chan string, 200_000)
	fileCmdChan := make(chan fileCmd, 1_000_000)
	posChan := make(chan positionStruct, 1_000_000)
	var wgWrite sync.WaitGroup
	var wgParse sync.WaitGroup
	//var seen sync.Map

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
		if len(strings.TrimSpace(line)) == 0 {
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
	close(fileCmdChan)
	close(posChan)
	wgWrite.Wait()

	err = pprof.WriteHeapProfile(memF)

	var count int
	db.QueryRow("SELECT COUNT(*) FROM positions").Scan(&count)
	fmt.Printf("Result: %v\n", count)
}

func pgnToFen(c <-chan string, wg *sync.WaitGroup, posChan chan positionStruct) {
	defer wg.Done()
	for pgnString := range c {
		pgn, err := chess.PGN(strings.NewReader(pgnString))
		if err != nil {
			fmt.Printf("Error parsing pgn: %v\n", err)
		}
		game := chess.NewGame(pgn)
		for i, position := range game.Positions() {
			// skip last "checkmate" board state
			if i == len(game.Positions())-1 {
				break
			}
			move := game.Moves()[i]
			var moveIdx int16 = -1
			if move.Promo() != chess.NoPieceType {
				moveIdx = getPromotionMoveIdx(move, position.Turn())
			} else {
				moveIdx = int16(move.S1())*64 + int16(move.S2()) + (8 * 3 * 4)
			}
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
			posChan <- positionStruct{position.String(), moveIdx, currentElo}
		}
	}
}

func databaseWriter(positionChan <-chan positionStruct, db *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()
	tx, err := db.Begin()
	check(err)
	storedGames := 0
	updatedGames := 0
	discardedGames := 0
	for pos := range positionChan {
		var storedElo int16
		err = tx.QueryRow("SELECT elo FROM positions WHERE fen = ?", pos.fen).Scan(&storedElo)
		if errors.Is(err, sql.ErrNoRows) {
			storedGames++
			// no position found so we insert
			_, err = tx.Exec("INSERT INTO positions (fen, move, elo) VALUES (?, ?, ?)", pos.fen, pos.move, pos.elo)
			if err != nil {
				tx.Rollback()
				fmt.Printf("ERROR in insert: %v\n", err)
			}
		} else if err == nil {
			// Update if new elo is higher
			if pos.elo > storedElo {
				updatedGames++
				_, err := tx.Exec("UPDATE positions SET move = ?, elo = ? WHERE fen = ?", pos.move, pos.elo, pos.fen)
				if err != nil {
					tx.Rollback()
					fmt.Printf("ERROR in update: %v\n", err)
				}
			} else {
				discardedGames++
			}
		} else {
			fmt.Printf("Error in DB writer: %v", err)
			continue
		}
		if storedGames%1_000_000 == 0 && storedGames != 0 {
			fmt.Printf("Pos inserted: %v, pos updated: %v\n, discarded: ", storedGames, updatedGames, discardedGames)
		}
	}
	err = tx.Commit()
	check(err)
}

func getPromotionMoveIdx(move *chess.Move, color chess.Color) int16 {
	var moveIdx int16 = -1
	startFile := int16(move.S1().File())
	endFile := int16(move.S2().File())
	diff := endFile - startFile + 1
	moveIdx = startFile*12 + diff*4

	// if white add to end of all move idx
	if color == chess.White {
		moveIdx += 64*64 + 87
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

func testPromotionIndex() {
	startRank := chess.Rank7
	startFiles := []chess.File{chess.FileA, chess.FileB, chess.FileC, chess.FileD, chess.FileE, chess.FileF, chess.FileG, chess.FileH}
	endRank := chess.Rank8

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

				fmt.Printf("Move: %v, Index: %v\n", move.String(), getPromotionMoveIdx(&move, chess.White))
			}
		}
	}
}
