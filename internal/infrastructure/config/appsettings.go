package config

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"
)

// Constantes para la construcción de consultas SQL

var (
	data string = "internal/infrastructure/data"
	path string = "appsetings.json"
)

// Estructura para la configuración de la aplicación
type AppSettings struct {
	Log struct {
		FileName     string `json:"FileName"`
		ConsoleLevel string `json:"ConsoleLevel"`
	} `json:"log"`
	Config struct {
		Port       string `json:"Port"`
		Protocolo  string `json:"Protocolo"`
		Host       string `json:"Host"`
		Name       string `json:"Name"`
		Enviroment string `json:"Enviroment"`
	} `json:"Config"`

	RequestBodyLimit   string   `json:"RequestBodyLimit"`
	RequestParamsLimit string   `json:"RequestParamsLimit"`
	AllowMethods       []string `json:"AllowMethods"`
	AllowHeaders       []string `json:"AllowHeaders"`

	Cors struct {
		AllowOrigins []struct {
			Origin           string `json:"Origin"`
			AllowCredentials bool   `json:"AllowCredentials"`
		}
	} `json:"Cors"`
	NameDb string `json:"nameDb"`
}

func AppSettingsUnmarshalnFn() *AppSettings {
	// Obtener la ruta del directorio del paquete utils
	originPath, err := filepath.Abs(".")

	if err != nil {

		return nil
	}
	// Construir la ruta al archivo JSON y moverse un directorio hacia arriba
	jsonFilePath := filepath.Join(originPath, data, path)

	// Leer y deserializar la configuración desde el archivo JSON
	data, err := ioutil.ReadFile(jsonFilePath)
	if err != nil {
		return nil
	}

	var config AppSettings
	if err := json.Unmarshal(data, &config); err != nil {
		return nil
	}

	return &config
}
