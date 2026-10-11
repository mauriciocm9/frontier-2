package main

import (
	"encoding/json"
	"strconv"
	"strings"

	"frontier/service/storage"
)

type result = storage.Result

func response(status int, body any) result {
	b, _ := json.Marshal(body)
	return result{Status: status, Body: b}
}

func failure(status int, code, message string) result {
	return response(status, map[string]string{"error": code, "mensaje": message})
}

type command struct {
	Kind   string
	Args   []string
	Amount int64
	Raw    string
}

func control(prefix string) int {
	remainder := 0
	for _, c := range prefix {
		if c >= 'a' && c <= 'z' { // ASCII-only case folding: 'ı' or 'ſ' must not count as letters
			c -= 'a' - 'A'
		}
		if c >= 'A' && c <= 'Z' {
			for _, d := range strconv.Itoa(int(c-'A') + 1) {
				remainder = (remainder*10 + int(d-'0')) % 97
			}
		} else if c >= '0' && c <= '9' {
			remainder = (remainder*10 + int(c-'0')) % 97
		}
	}
	return 98 - (remainder*100)%97
}

func digits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func parseMessage(m string) (command, *result) {
	bad := func(code, message string) (command, *result) {
		r := failure(400, code, message)
		return command{}, &r
	}
	parts := strings.Split(m, ";")
	if len(parts) < 2 {
		return bad("E103", "falta control")
	}
	for _, part := range parts {
		if part == "" {
			return bad("E104", "segmento vacío")
		}
	}
	kind := strings.ToUpper(parts[0])
	check := parts[len(parts)-1]
	if kind != "PING" {
		if len(check) > 2 || !digits(check) {
			return bad("E201", "control mal formado")
		}
		n, _ := strconv.Atoi(check)
		if control(strings.Join(parts[:len(parts)-1], ";")) != n {
			return bad("E202", "control inválido")
		}
	}
	arity := map[string]int{"PING": 2, "ALTA": 3, "CONSULTA": 3, "DEPOSITO": 4, "TRANSFER": 5, "ANULA": 3}
	n, known := arity[kind]
	if !known {
		body := map[string]string{"error": "E301", "mensaje": "comando desconocido "}
		best := 3
		for _, candidate := range []string{"PING", "ALTA", "CONSULTA", "DEPOSITO", "TRANSFER", "ANULA"} {
			if distance := editDistance(kind, candidate); distance < best {
				best = distance
				body["sugerencia"] = candidate
			}
		}
		r := response(400, body)
		return command{}, &r
	}
	if len(parts) != n {
		return bad("E302", "aridad inválida")
	}
	c := command{Kind: kind, Args: parts[1 : len(parts)-1], Raw: m}
	if kind == "DEPOSITO" || kind == "TRANSFER" {
		amount := c.Args[len(c.Args)-1]
		value, err := strconv.ParseInt(amount, 10, 64)
		if !digits(amount) || (len(amount) > 1 && amount[0] == '0') || err != nil || value <= 0 || value > 9999999999 {
			return bad("E303", "monto inválido")
		}
		c.Amount = value
	}
	if kind == "TRANSFER" && c.Args[0] == c.Args[1] {
		return bad("E304", "origen y destino iguales")
	}
	return c, nil
}

func editDistance(a, b string) int {
	x, y := []rune(a), []rune(b)
	row := make([]int, len(y)+1)
	for j := range row {
		row[j] = j
	}
	for i, c := range x {
		previous := row[0]
		row[0] = i + 1
		for j, d := range y {
			old := row[j+1]
			cost := 0
			if c != d {
				cost = 1
			}
			row[j+1] = min(row[j+1]+1, row[j]+1, previous+cost)
			previous = old
		}
	}
	return row[len(y)]
}
