package main

import (
	// "encoding/json"
	// "log"

	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func getNodeID() int {
	// read node id from "node.txt" and increment it
	// if it doesn't exist, create it with value 1
	// return the node id
	node_id := 1
	if _, err := os.Stat("nodes.txt"); os.IsNotExist(err) {
		// file does not exist
		file, err := os.Create("nodes.txt")
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
	} else {
		// file exists
		file, err := os.Open("nodes.txt")
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		// read the node id
		_, err = fmt.Fscanf(file, "%d", &node_id)
		if err != nil {
			log.Fatal(err)
		}
	}

	// increment the node id
	node_id++
	// write the node id back to the file
	file, err := os.Create("nodes.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	file.WriteString(strconv.Itoa(node_id))

	return node_id
}

func main() {
	// initialize a new static counter per node
	n := maelstrom.NewNode()
	var counter int
	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// generate an unique id
		// get timestamp epoch
		// tx := time.Now().UnixNano()
		node_id := n.ID()
		// id := fmt.Sprintf("%d-%d-%d", getNodeID(), counter, tx)
		id := fmt.Sprintf("%d-%s", counter, node_id)
		counter++

		body["type"] = "generate_ok"
		body["id"] = id

		// send the response
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
