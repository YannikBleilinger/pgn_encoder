package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/corentings/chess"
	"os"
	"runtime"
	"sync"
)

//TIP <p>To run your code, right-click the code and select <b>Run</b>.</p> <p>Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.</p>

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	dataPath := "D:\\ChessGPT Data\\Okt_2024\\data.pgn"
	outPath := ".\\files\\encoded_positions.bin"
	numWorkers := runtime.NumCPU() - 1
	f, err := os.Open(dataPath)
	check(err)

	defer func(f *os.File) {
		err := f.Close()
		check(err)
	}(f)

	scanner := chess.NewScanner(f)
	gameChan := make(chan *chess.Game, 100)
	outChan := make(chan []byte, 1000)
	var wg sync.WaitGroup
	var seen sync.Map

	wg.Add(1)
	go writer(&wg, outPath, outChan)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go processPGN(gameChan, outChan, &wg, &seen)
	}
	totalGames := 200_000
	currentGames := 0
	fmt.Println("Starting to process games")
	for scanner.Scan() {
		game := scanner.Next()
		gameChan <- game
		currentGames++
		if currentGames%25_000 == 0 {
			fmt.Printf("Games processes: %v\n", currentGames)
		}
		if currentGames == totalGames {
			break
		}
	}
	close(gameChan)
	wg.Wait()
}

func processPGN(c chan *chess.Game, outChan chan []byte, wg *sync.WaitGroup, seen *sync.Map) {
	defer wg.Done()
	for game := range c {
		for i, position := range game.Positions() {
			// skip last "checkmate" board state
			if i == len(game.Positions())-1 {
				break
			}
			move := game.Moves()[i] // move for current position
			moveStart := int8(move.S1())
			moveEnd := int8(move.S2())
			var encodedBoard [65][7]int8
			encodedBoard[0] = getGlobalVector(position)
			for square, piece := range position.Board().SquareMap() {
				v := getSquareVector(piece)
				// add vector to map at correct position - empty squares are 0 vectors
				// index 0 is for global information
				encodedBoard[int(square)+1] = v
			}

			var buf bytes.Buffer
			// serialize board
			for _, row := range encodedBoard {
				for _, val := range row {
					err := binary.Write(&buf, binary.LittleEndian, val)
					check(err)
				}
			}
			// serialize start and end square of move
			err := binary.Write(&buf, binary.LittleEndian, moveStart)
			check(err)
			err = binary.Write(&buf, binary.LittleEndian, moveEnd)
			check(err)

			data := buf.Bytes()
			hash := sha256.Sum256(data)

			if _, loaded := seen.LoadOrStore(hash, true); !loaded {
				outChan <- data
			}
		}
	}
}

// vector: [color, isKing, isQueen, isRook, isBishop, isKnight, isPawn]
func getSquareVector(piece chess.Piece) [7]int8 {
	var vector [7]int8
	// set color at index 0
	if piece.Color() == chess.White {
		vector[0] = 1
	} else {
		vector[0] = -1
	}
	index := -1
	switch piece.Type() {
	case chess.King:
		index = 1
	case chess.Queen:
		index = 2
	case chess.Rook:
		index = 3
	case chess.Bishop:
		index = 4
	case chess.Knight:
		index = 5
	case chess.Pawn:
		index = 6
	default:
		panic("Given piece has no correct type: " + piece.Type().String())
	}
	vector[index] = 1
	return vector
}

func getGlobalVector(position *chess.Position) [7]int8 {
	var vector [7]int8
	var castlingRights = position.CastleRights()
	// set castling rights
	if castlingRights.CanCastle(chess.White, chess.KingSide) {
		vector[0] = 1
	}
	if castlingRights.CanCastle(chess.White, chess.QueenSide) {
		vector[1] = 1
	}
	if castlingRights.CanCastle(chess.Black, chess.KingSide) {
		vector[2] = 1
	}
	if castlingRights.CanCastle(chess.Black, chess.QueenSide) {
		vector[3] = 1
	}

	// set en passant square
	enPassantSquare := position.EnPassantSquare()
	if enPassantSquare != chess.NoSquare {
		vector[4] = int8(enPassantSquare)
	} else {
		vector[4] = 0
	}
	// set turn
	if position.Turn() == chess.White {
		vector[5] = 1
	} else {
		vector[5] = -1
	}
	return vector
}

func getOutputVector() {
	// first try: we encode a 2 element vector with start square and end square [8,16]
}

func writer(wg *sync.WaitGroup, outPath string, outChan chan []byte) {
	defer wg.Done()
	outFile, err := os.Create(outPath)
	check(err)
	defer outFile.Close()

	for data := range outChan {
		_, err := outFile.Write(data)
		check(err)
	}
}
