package cortexanalystapp

import (
	"context"
	"embed"
	"io/fs"
	"log"
	"net/http"
	"syscall"

	"github.com/spf13/cobra"
)

var (
	//go:embed ui/dist/*
	uiDir embed.FS
)

func Cmd() *cobra.Command {
	var flags struct {
		http   string
		config YAMLFlag[config]
	}
	cmd := &cobra.Command{
		Use:   "cortex-analyst-demo",
		Short: "",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()
			OnSignal(cancel, syscall.SIGINT, syscall.SIGTERM)

			staticFS, err := fs.Sub(uiDir, "ui/dist")
			if err != nil {
				log.Fatal(err)
			}

			mux := http.NewServeMux()
			mux.Handle("/", http.FileServerFS(staticFS))

			privKey, err := readPrivateKey()
			if err != nil {
				return err
			}

			apiServer, err := newAPI(ctx, privKey, flags.config.Value)
			if err != nil {
				return err
			}
			mux.Handle("/api/", apiServer)

			log.Printf("Starting HTTP server on %s", flags.http)
			if err := http.ListenAndServe(flags.http, mux); err != nil {
				return err
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&flags.http, "http", ":8080", "Address on which the HTTP server will listen.")
	cmd.Flags().Var(&flags.config, "config", "The config")
	return cmd
}

