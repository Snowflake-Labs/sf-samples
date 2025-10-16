package cortexanalystapp

type config struct {
	Models []modelConfig `yaml:"models"`
}

type modelConfig struct {
	Name               string   `yaml:"name"`
	Path               string   `yaml:"path"`
	Description        string   `yaml:"description"`
	SuggestedQuestions []string `yaml:"suggested_questions"`
}
