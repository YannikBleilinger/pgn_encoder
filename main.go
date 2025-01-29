package main

import (
	"fmt"
	"github.com/corentings/chess"
	"os"
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
	path := "D:\\ChessGPT Data\\Okt_2024\\data.pgn"

	f, err := os.Open(path)
	check(err)
	defer f.Close()

	scanner := chess.NewScanner(f)
	c := make(chan *chess.Game, 10)
	var wg sync.WaitGroup
	wg.Add(1)
	totalGames := 1
	currentGames := 0
	go processPGN(c, &wg)
	for scanner.Scan() {
		game := scanner.Next()
		c <- game
		currentGames++
		if currentGames == totalGames {
			break
		}
	}
	close(c)
	wg.Wait()
}

func processPGN(c chan *chess.Game, wg *sync.WaitGroup) {
	defer wg.Done()
	//encodeBoard := make([][]int8, 65)
	for game := range c {
		for _, position := range game.Positions() {
			for _, square := range position.Board().SquareMap() {
				fmt.Println(square.String())
			}
		}
	}
}
