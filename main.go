package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
)

func getSlotHealthCheck(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	slotName := params["slot_name"]
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
		"slot":   slotName,
	})
}

func main() {
	fmt.Println("Hello")
	router := mux.NewRouter()
	router.HandleFunc("/slot/{slot_name}/health_check", getSlotHealthCheck).Methods("GET")
	http.ListenAndServe(":8000", router)
}
