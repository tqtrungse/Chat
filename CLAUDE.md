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
- The change affects a public API.
- The change is hard to reverse. Examples: a schema migration, a deletion of files or data, a new dependency, a
  change to the deploy configuration.

Do these steps when you ask a question:

1. Give the user 2 to 4 options. Mark the option that you recommend. Give one short reason for this option.
2. Add the option "Other". This option lets the user give a different answer.
3. State each assumption that you made.

Continue the work that does not depend on the answer, while you wait for the answer.