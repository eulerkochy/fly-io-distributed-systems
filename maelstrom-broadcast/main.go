package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	var messages []any
	// used in the topology handler
	var neighbor_list []string

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		receiver := msg.Src
		log.Println("broadcast message received from: ", receiver)
		log.Println("broadcast message received with body: ", body)

		bcast_message := body["message"]
		messages = append(messages, bcast_message)

		// delete the message from the body
		delete(body, "message")

		// we need to broadcast the message to all the neighbors
		// we can chunk up the message and send it to each neighbor in batches

		// Get the list for this node
		go func() {
			for _, neighbor := range neighbor_list {
				// do not send message back to the sender
				if neighbor == receiver {
					continue
				}
				log.Printf("neighbor: %s", neighbor)
				log.Printf("message: %v", bcast_message)

				broadcast_message := map[string]any{
					"type":    "broadcast",
					"message": bcast_message,
				}

				log.Printf("broadcast message: %v", broadcast_message)

				// Send the message to the neighbor
				if err := n.Send(neighbor, broadcast_message); err != nil {
					log.Printf("failed to send message to %s: %v", neighbor, err)
				}
			}
		}()

		// Update the message type to return back.
		body["type"] = "broadcast_ok"
		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = messages

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "topology_ok"
		// "topology":{"n0":[]},"msg_id":1}
		topology := body["topology"].(map[string]any)
		// Get the list for this node
		list := topology[n.ID()].([]any)

		// Convert each element to string and append
		for _, node := range list {
			neighbor_list = append(neighbor_list, node.(string))
		}

		// remove topolgy from the body
		delete(body, "topology")

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
