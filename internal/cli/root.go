package cli

import "github.com/spf13/cobra"

// NewRootCmd builds the top-level command and wires all subcommands.
//
// Keeping command registration in one place makes the CLI structure explicit
// and easier to extend without touching execution/bootstrap code.
func NewRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "pulsar-cli",
		Short: "CLI Pulsar client supporting reader, consumer, and producer",
	}

	rootCmd.AddCommand(readerCmd())
	rootCmd.AddCommand(consumerCmd())
	rootCmd.AddCommand(producerCmd())
	rootCmd.AddCommand(roundtripCmd())

	return rootCmd
}
