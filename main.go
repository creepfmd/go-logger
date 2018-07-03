package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// our main function
func main() {
	log.Println("Connecting mongo at " + os.Getenv("MONGO_URL"))
	session, err := mgo.Dial(os.Getenv("MONGO_URL"))
	if err != nil {
		log.Fatal("Error connecting")
		panic(err)
	}
	defer session.Close()
	log.Println("Setting collection...")
	c := session.DB("rutt").C("logs")
	log.Println("Setting router...")
	router := mux.NewRouter()
	router.HandleFunc("/new/{correlationId}/{sourceId}", func(w http.ResponseWriter, r *http.Request) {
		newMessage(w, r, c)
	}).Methods("GET")
	router.HandleFunc("/queued/{correlationId}/{timeQueued}", func(w http.ResponseWriter, r *http.Request) {
		messageQueued(w, r, c)
	}).Methods("GET")
	router.HandleFunc("/update/{correlationId}/{field}/{value}", func(w http.ResponseWriter, r *http.Request) {
		messageUpdate(w, r, c)
	}).Methods("GET")
	router.HandleFunc("/destinationAdded/{correlationId}/{destinationId}/{messageId}/{timeQueued}", func(w http.ResponseWriter, r *http.Request) {
		destinationAdded(w, r, c)
	}).Methods("GET")
	router.HandleFunc("/destinationUpdated/{correlationId}/{destinationId}/{messageId}/{field}/{value}", func(w http.ResponseWriter, r *http.Request) {
		destinationUpdated(w, r, c)
	}).Methods("GET")
	log.Println("Listening 8084...")
	log.Fatal(http.ListenAndServe(":8084", router))
}

func newMessage(w http.ResponseWriter, r *http.Request, c *mgo.Collection) {
	params := mux.Vars(r)
	err := insertMongo(c, params["correlationId"], params["sourceId"])
	if err != nil {
		http.Error(w, err.Error(), 500)
	} else {
		json.NewEncoder(w).Encode(params)
	}
}

func messageQueued(w http.ResponseWriter, r *http.Request, c *mgo.Collection) {
	params := mux.Vars(r)
	err := upsertMongo(c, params["correlationId"], bson.M{"timeQueued": params["timeQueued"]})
	if err != nil {
		http.Error(w, err.Error(), 500)
	} else {
		json.NewEncoder(w).Encode(params)
	}
}

func messageUpdate(w http.ResponseWriter, r *http.Request, c *mgo.Collection) {
	params := mux.Vars(r)
	err := upsertMongo(c, params["correlationId"], bson.M{params["field"]: params["value"]})
	if err != nil {
		http.Error(w, err.Error(), 500)
	} else {
		json.NewEncoder(w).Encode(params)
	}
}

func destinationAdded(w http.ResponseWriter, r *http.Request, c *mgo.Collection) {
	params := mux.Vars(r)
	err := upsertMongo(c, params["correlationId"], bson.M{"destinations." + params["destinationId"] + "." + params["messageId"] + ".timeQueued": params["timeQueued"]})
	if err != nil {
		http.Error(w, err.Error(), 500)
	} else {
		json.NewEncoder(w).Encode(params)
	}
}

func destinationUpdated(w http.ResponseWriter, r *http.Request, c *mgo.Collection) {
	params := mux.Vars(r)
	err := upsertMongo(c, params["correlationId"], bson.M{"destinations." + params["destinationId"] + "." + params["messageId"] + "." + params["field"]: params["value"]})
	if err != nil {
		http.Error(w, err.Error(), 500)
	} else {
		json.NewEncoder(w).Encode(params)
	}
}

func insertMongo(c *mgo.Collection, id string, sourceID string) error {
	err := c.Insert(bson.M{"_id": id, "sourceId": sourceID})
	if err != nil {
		log.Println(err)
	}
	return err
}

func upsertMongo(c *mgo.Collection, id string, docObject bson.M) error {
	err := c.Update(bson.M{"_id": id}, bson.M{"$set": docObject})
	if err != nil {
		log.Println(err)
	}
	return err
}
