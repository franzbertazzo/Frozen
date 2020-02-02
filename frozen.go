package main

import (
	"fmt"
	"net"
	"io"
	"bufio"
	"strings"
)

type User 			struct {
	Username		string
	Nickname		string
	Password		string
	Output chan		Message
	CurrentChatRoom ChatRoom
}

type Message 		struct {
	Username 		string
	Text     		string
}

type Request		struct {
	Person			*User
	RoomName		string
}

type ChatServer		struct {
	AddUsr			chan User
	AddNick			chan User
	RemoveNick		chan User
	NickMap			map[string]User
	Users			map[string]User
	Rooms			map[string]ChatRoom
	Create			chan ChatRoom
	Delete			chan ChatRoom
	UsrJoin			chan Request
	UsrLeave		chan Request
}

type ChatRoom	 	struct {
	Name  			string
	Users 			map[string]User
	Join  			chan User
	Leave 			chan User
	Input 			chan Message
}

//handles the Server
func handleConnServer(conn net.Conn, server *ChatServer) {

	var user User

	// Output a message to the new connected user
	io.WriteString(conn, "Enter a username: ")

	// Create a scanner to scan the new user inputs
	scan := bufio.NewScanner(conn)

	// First we handle the user connection
	scan.Scan()
	username := scan.Text()

	if tmp, check := server.Users[username]; check {
		user = tmp
		// If the user already has an account I ask for his password
		io.WriteString(conn, "Enter your password: ")
		scan.Scan()
		password := scan.Text()

		for password != user.Password {
			io.WriteString(conn, "Wrong password. Try again ")
			scan.Scan()
			password = scan.Text()
		}

	} else {
		// If the user doesnt have an account then I ask ask him to create an account

		io.WriteString(conn, "Enter a nickname: ")
		scan.Scan()
		nickname := scan.Text()

		// Looping until the user finds a none taken nickname
		for {
			if _, check := server.NickMap[nickname]; check {
				io.WriteString(conn, "Nickname already taken. Try again: ")
				scan.Scan()
				nickname = scan.Text()
			} else {
				break
			}
		}

		// Now the user chooses a password
		io.WriteString(conn, "Create a password for your account: ")
		scan.Scan()
		password := scan.Text()

		// And finally we add the new user in the list of users in the server
		tmp := User {
			Username: username,
			Nickname: nickname,
			Password: password,
			Output: make(chan Message),
		}
		server.AddUsr <- tmp
		user = tmp
	}

	// Now that we have a connected user we ask hin to join a chat room
	io.WriteString(conn, "Join a chat room: ")
	scan.Scan()
	room_name := scan.Text()

	// Creating a request to join a chat room
	request := Request{
		Person: &user,
		RoomName: room_name,
	}
	// Joining the room
	server.UsrJoin <- request
	// Create a defer to leave the room after function returns
	defer func() {
		server.UsrLeave <- request
	}()
	
	go func() {

		io.WriteString(conn, "You joined " + room_name + "\n")
		for scan.Scan() {
			line := scan.Text()
			words := strings.Split(line, " ")
			if (line == "WHOAMI") {
				user.Output <- Message{
					Username: "SYSTEM",
					Text: "\nusername: "+user.Username+"\nnickname: "+user.Nickname+"\ncurrent room: "+user.CurrentChatRoom.Name,
				}
			} else if words[0] == "NICK" && len(words) > 1 {
				i := 0
				for _, p := range server.Users {
					if i != 0 {
						break
					} else if p.Nickname == words[1] {
						user.Output <- Message{
							Username: "SYSTEM",
							Text: "nickname \""+words[1]+"\" taken",
						}
						i = 1
					}
				}

				if _, test := server.NickMap[words[1]]; test {
					i = 2
				}
				if i == 0 {
					server.RemoveNick <- user
					delete(server.NickMap, user.Nickname)
					server.NickMap[words[1]] = user
					user.Nickname = words[1]
					server.RemoveNick <- user
				}
			} else if line == "NAMES" {
				for person := range server.Users {
					user.Output <- Message{
						Username: "SYSTEM",
						Text: person,
					}
				}
			} else if line == "ROOMMATES" {
				for _, person := range user.CurrentChatRoom.Users {
					user.Output <- Message{
						Username: "SYSTEM",
						Text: person.Nickname,
					}
				}
			} else if words[0] == "PRIVMSG" && len(words) > 2 {
				if words[1] == "USER" {
					usr, ok := server.Users[words[2]]
					if ok {
						usr.Output <- Message{
							Username: user.Username,
							Text: line,
						}
					} else {
						user.Output <- Message{
							Username: "SYSTEM",
							Text: "User not found",
						}
					}
				} else if words[1] == "CHAN" {
					room, ok := server.Rooms[words[2]]
					if ok {
						room.Input <- Message{
							Username: user.Username,
							Text: line,
						}
					} else {
						user.Output <- Message{
							Username: user.Username,
							Text: "Room not found",
						}
					}
				} else {
					user.Output <- Message{
						Username: "SYSTEM",
						Text: "Invalid option",
					}
				}
			} else if line == "LIST" {
				for room := range server.Rooms {
					user.Output <- Message{
						Username: "SYSTEM",
						Text: room,
					}
				}
			} else if words[0] == "JOIN" && len(words) > 1 {
				request = Request{
					Person:   &user,
					RoomName: user.CurrentChatRoom.Name,
				}
				server.UsrLeave <- request
				request = Request{
					Person:   &user,
					RoomName: words[1],
				}
				server.UsrJoin <- request
			} else if line == "PART" {
				request = Request{
					Person:   &user,
					RoomName: user.CurrentChatRoom.Name,
				}
				server.UsrLeave <- request
				request = Request{
					Person:   &user,
					RoomName: "lobby",
				}
				server.UsrJoin <- request
			} else {
				user.CurrentChatRoom.Input <- Message{user.Nickname, line}
			}
		}

	}()
	
	for msg := range user.Output {
		if msg.Username != user.Username {
			_, err := io.WriteString(conn, msg.Username+": "+msg.Text+"\n")
			if err != nil {
				break
			}
		}
	}

}

//creates a new Server
func createChatServer(capacity int) *ChatServer {
	server := &ChatServer{
		AddUsr: make(chan User, capacity),
		AddNick: make(chan User, capacity),
		RemoveNick: make(chan User, capacity),
		NickMap: make(map[string]User),
		Users: make(map[string]User),
		Rooms: make(map[string]ChatRoom),
		Create: make(chan ChatRoom, capacity),
		Delete: make(chan ChatRoom, capacity),
		UsrJoin: make(chan Request, capacity),
		UsrLeave: make(chan Request, capacity),
	}
	return server
}

//Start a room
func (room *ChatRoom) start() {
	for {
		select {
		case user := <- room.Join:
			room.Users[user.Username] = user
			room.Input <- Message{
				Username: "SYSTEM",
				Text: user.Nickname+" joined "+room.Name,
			}
		case user := <- room.Leave:
			delete(room.Users, user.Username)
			room.Input <- Message{
				Username: "SYSTEM",
				Text: user.Nickname+" left "+room.Name,
			}
		case message := <- room.Input:
			for _, user := range room.Users {
				select {
				case user.Output <- message:
				default:
				}
			}
		}
	}
}

//Start the main server
func (server *ChatServer) start() {

	capacity := 4

	for {
		select {

		case user := <- server.AddUsr:
			server.Users[user.Username] = user
			server.NickMap[user.Nickname] = user

		case user := <- server.AddNick:
			server.NickMap[user.Nickname] = user
		case user := <- server.RemoveNick:
			delete(server.NickMap, user.Nickname)

		case room := <- server.Create:
			fmt.Println("New room created")
			server.Rooms[room.Name] = room
			go room.start()

		case room := <- server.Delete:
			delete(server.Rooms, room.Name)

		case request := <- server.UsrJoin:
			if chatRoom, test := server.Rooms[request.RoomName]; test {
				chatRoom.Join <- *(request.Person)
				request.Person.CurrentChatRoom = chatRoom
			} else {
				newRoom := ChatRoom{
					Name:  request.RoomName,
					Users: make(map[string]User),
					Join:  make(chan User, capacity),
					Leave: make(chan User, capacity),
					Input: make(chan Message, capacity),
				}
				server.Rooms[request.RoomName] = newRoom
				server.Create <- newRoom
				newRoom.Join <- *(request.Person)
				request.Person.CurrentChatRoom = newRoom
			}

		case request := <- server.UsrLeave:
			room := server.Rooms[request.RoomName]
			room.Leave <- *(request.Person)

		}
	}
}

func main() {
	// listening on Port 8080
	ln, err := net.Listen("tcp", ":8080")
	defer ln.Close()

	if err != nil {
		fmt.Println("Error")
	}

	// Creating and starting a new server
	server := createChatServer(4);
	go server.start()
	fmt.Println("Running on localhost:8080")

	// For each new connection call handleConnServer()
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error")
		}

		go handleConnServer(conn, server)
	}

}