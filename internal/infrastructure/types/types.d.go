package types

import (
	"github.com/gofiber/fiber/v2"
)

type HandlerModule struct {
	Handler fiber.Handler // Cambiamos a http.HandlerFunc
	Route   string
	Method  string // Cambiamos a string
}

type SliceHandlers struct {
	Prefix string
	Routes []HandlerModule
}

type GlobalHandlers []SliceHandlers

type HandlersStore struct {
	Handlers []SliceHandlers
}

func NewHandlersStore() *HandlersStore {
	return &HandlersStore{}
}
