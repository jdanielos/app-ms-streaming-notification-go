package config

import "net/http"

// AppError representa el error personalizado similar al enum de Rust
type AppError struct {
	Message      string `json:"message"`
	Code         int    `json:"code"`
	InternalCode string `json:"internal_code"`
	Status       int    `json:"status"`
}

// Implementa la interfaz 'error' de Go
func (e *AppError) Error() string {
	return e.Message
}

// ResponseSystem es el envoltorio estándar para todas las respuestas (éxito y error)
type ResponseSystem struct {
	Success      bool        `json:"success"`
	Data         interface{} `json:"data"`
	Message      string      `json:"message"`
	Code         int         `json:"code"`
	InternalCode string      `json:"internal_code"`
	Status       int         `json:"status"`
}

// Códigos de error (Equivalente a impl AppError { pub fn code(...) })
const (
	ErrCodeAuthInvalidCredentials                = 1001
	ErrCodeEntitiesDataInvalid                   = 1002
	ErrCodeBadRequestDataSystem                  = 1004
	ErrCodeInternalServer                        = 5000
	ErrCodeUnprocessableEntityInvalidDataRequest = 1011
)

// NewAuthInvalidCredentials imita a Self::AuthInvalidCredentials(String)
func NewAuthInvalidCredentials(detail string) *AppError {
	return &AppError{
		Message:      "Credenciales inválidas: " + detail,
		Code:         ErrCodeAuthInvalidCredentials,
		InternalCode: "AUTH_INVALID_CREDENTIALS",
		Status:       http.StatusUnauthorized,
	}
}

func NewInternalServerError(err error) *AppError {
	return &AppError{
		Message:      "Error interno del servidor: " + err.Error(),
		Code:         ErrCodeInternalServer,
		InternalCode: "INTERNAL_SERVER_ERROR",
		Status:       http.StatusInternalServerError,
	}
}

func NewUnprStatusUnprocessableEntity(err error) *AppError {
	return &AppError{
		Message:      "Error interno no se pudo formatear el json del body " + err.Error(),
		Code:         ErrCodeUnprocessableEntityInvalidDataRequest,
		InternalCode: "INTERNAL_ENTITY_SERVER",
		Status:       http.StatusUnprocessableEntity,
	}
}
func NewErrCodeBadRequestDataSystem(err error) *AppError {
	return &AppError{
		Message:      "error al procesar la solicitud intente nuevamente " + err.Error(),
		Code:         ErrCodeBadRequestDataSystem,
		InternalCode: "ERROR_BAD_REQUEST",
		Status:       http.StatusUnprocessableEntity,
	}
}
func NewErrCodeEntitiesDataInvalid(err error) *AppError {
	return &AppError{
		Message:      "Error de entidades informacion no valida " + err.Error(),
		Code:         ErrCodeEntitiesDataInvalid,
		InternalCode: "INTERNAL_ENTITY_SERVER",
		Status:       http.StatusUnprocessableEntity,
	}
}
