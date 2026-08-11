package dto

type SendDtoRequest struct {
	Email        string `json:"email"`
	Code         string `json:"code"`
	// Le faltaba el `json:` y la comilla de apertura: `xfingerprint"`. Con la
	// etiqueta rota, encoding/json la ignora y cae al nombre del campo, asi que
	// un JSON con "xfingerprint" en minusculas nunca llenaba este campo.
	XFingerprint string `json:"xfingerprint"`
}
