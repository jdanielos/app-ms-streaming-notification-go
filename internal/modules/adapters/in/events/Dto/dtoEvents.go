package dto

import "github.com/streamingNotifyHub/internal/modules/domains/entities/command"

type AuthEventDTO struct {
	Email                string `json:"email"`
	Code                 string `json:"code"`
	XFingerPrint         string `json:"xfingerprint"`
	TimeCodeVerification string `json:"time_code_verification"`
	Subject              string `json:"subject"`
	Metadata             map[string]string
	TypeTemplated        string `json:"type_templated"`
	ChallengeID          string `json:"challenge_id"`
}

func (dto AuthEventDTO) ToCommand() command.AuthenticatedUserCommand {
	return command.AuthenticatedUserCommand{
		Email:                dto.Email,
		Code:                 dto.Code,
		XFingerPrint:         dto.XFingerPrint,
		Subject:              dto.Subject,
		TimeCodeVerification: dto.TimeCodeVerification,
		Metadata:             dto.Metadata,
		TypeTemplated:        dto.TypeTemplated,
		ChallengeID:          dto.ChallengeID,
	}
}
