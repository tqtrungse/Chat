## Project

Go module. Layout:

- `cmd/`: service entry points.
- `internal/`: private service code.
- `pkg/`: shared library packages.
- `api/`: API definitions.
- `deploy/`: deploy and infrastructure configuration.

Commands:

```bash
go build ./...
go vet ./...
go test ./...
go test ./pkg/<name>/...   # one package
```

## Comments

Write and edit all code comments in ASD-STE100 (Simplified Technical English). This includes Go doc comments.

1. Write short sentences. Use one idea in each sentence. Use no more than 25 words in a sentence.
2. Use the active voice. Use the simple present tense to describe what the code does.
3. Use one word for one meaning. Use the same word for the same thing in all comments.
4. Use `must` for a requirement. Use `do not` for a prohibition. Do not use `should` or `may` for a requirement.
5. Do not use phrasal verbs. Do not use the -ing form of a verb as a verb.
6. Keep technical names unchanged. These names include identifiers, protocol names, and error messages.
7. Keep the Go doc comment format. Start the comment with the name of the symbol. Keep the lines `Default:`,
   `Required.`, and `Optional.` where they exist.
8. Write the reason for the code, not a repeat of the code. Do not write a comment that only says what the code
   shows.
9. Write or change only the comments that the current task needs. Do not rewrite other comments unless the user
   asks for it.
10. Do not change the meaning of a comment. If the meaning is not clear, ask the user (see "When Unsure").

## When Unsure

Ask the user before you continue, when one of these conditions is true:

- The request has two or more reasonable meanings.
- The change breaks a public API. Examples: a change to the signature or the behavior of an exported symbol in `pkg/`.
- The change is hard to reverse. Examples: a schema migration, a deletion of files or data, a new dependency, a
  change to the deploy configuration.

Do these steps when you ask a question:

1. Give the user 2 to 4 options. Mark the option that you recommend. Give one short reason for this option.
2. State each assumption that you made.

<!-- codegraph:start -->
## CodeGraph

This section applies when the CodeGraph MCP tools are available. Only `codegraph explore` is enabled by default.
Do not assume that the other `codegraph *` tools exist.

- Before you edit a function, class, or method, call `codegraph explore` with its name. Check the callers, the
  callees, and the affected execution paths.
- Skip this step for edits to comments, docs, and test-only code.
- Before you start the edit, state the blast radius and the risk level (see the table below).
- To learn unfamiliar code, call `codegraph explore` with a question before you use grep. Do not repeat a search
  that CodeGraph already answered.
- To rename a symbol, use a language-aware tool (`gopls rename` or the GoLand `rename_refactoring` tool). Do not
  use find-and-replace. Review the full diff after the rename.
- To extract or move code, call `codegraph explore` first to find the external callers.
- To debug, call `codegraph explore` with the error or the symptom. Trace the path from the entry point to the
  failure. For a regression, read `git diff` first.
- Before you commit, read `git diff` and confirm that the changes match the expected scope. A successful
  CodeGraph query does not prove that the code is correct. Run the tests.

| Risk     | Meaning                                              | Action                              |
| -------- | ---------------------------------------------------- | ----------------------------------- |
| LOW      | Isolated code with few callers                       | Run the relevant unit tests         |
| MEDIUM   | Several related symbols can be affected              | Test the affected components        |
| HIGH     | Important execution paths or many components         | Warn the user. Review with care.    |
| CRITICAL | Core business logic or critical execution paths      | Warn the user. Get confirmation.    |

The risk level is an engineering assessment. It is not a CodeGraph output.

If a CodeGraph tool reports that the index is out of date:

1. Wait for the automatic sync (about 2 seconds) and retry the tool.
2. If the index is still out of date, run `codegraph sync` and retry.
3. Use `codegraph index --force` only when a full re-index is necessary.

Do not run `codegraph sync` after every edit. Use `codegraph status` to check the index health.
<!-- codegraph:end -->
