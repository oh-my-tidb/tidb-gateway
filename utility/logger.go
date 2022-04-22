package utility

import "go.uber.org/zap"

func GetLogger() *zap.SugaredLogger {
	logger, _ := zap.NewDevelopment()
	return logger.Sugar()
}
