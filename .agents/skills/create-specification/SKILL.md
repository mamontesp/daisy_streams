---
name: create-specification
description: >-
  Create a new specification file for the solution, optimized for Generative AI
  consumption. Use this skill when the user asks to create a spec, specification,
  requirements document, or design document for a component, feature, or system.
  The skill produces a structured Markdown file in the /spec/ directory following
  a standardized template with front matter, requirements, interfaces, acceptance
  criteria, and test strategy sections.
---

# Create Specification

Create a new specification file that defines requirements, constraints, and
interfaces for a solution component. The specification must be clear,
unambiguous, and structured for effective use by Generative AIs.

## Prerequisites

Before creating the specification, gather the following from the user:

1. **Specification Purpose** — what the spec covers (e.g., "authentication
   service", "data ingestion pipeline").
2. **High-Level Category** — one of: `schema`, `tool`, `data`, `infrastructure`,
   `process`, `architecture`, or `design`.
3. **Key requirements and constraints** — any known requirements, business rules,
   or technical constraints the user wants captured.

If the user has not provided these, ask clarifying questions before proceeding.

## Steps

### 1. Determine the File Name

Construct the file name using the pattern:

```
spec-<category>-<descriptive-slug>.md
```

Where:
- `<category>` is one of: `schema`, `tool`, `data`, `infrastructure`, `process`,
  `architecture`, `design`
- `<descriptive-slug>` uses lowercase letters, digits, and hyphens only
- Example: `spec-architecture-auth-service.md`

### 2. Ensure the `/spec/` Directory Exists

Create the `/spec/` directory at the workspace root if it does not already exist.

### 3. Write the Specification

Use the template in [resources/spec-template.md](./resources/spec-template.md)
as the base. Fill in every section with content relevant to the user's request.

### 4. Best Practices for AI-Ready Content

When writing the specification content, follow these rules:

- Use precise, explicit, and unambiguous language.
- Clearly distinguish between requirements (`REQ-`), constraints (`CON-`),
  guidelines (`GUD-`), security requirements (`SEC-`), and patterns (`PAT-`).
- Use structured formatting (headings, lists, tables) for easy parsing.
- Avoid idioms, metaphors, or context-dependent references.
- Define all acronyms and domain-specific terms in the Definitions section.
- Include examples and edge cases in Section 9.
- Ensure the document is self-contained and does not rely on external context.
- Use Given-When-Then format for acceptance criteria where appropriate.

### 5. Validate the Output

After writing the spec, verify:

- [ ] Front matter contains at minimum: `title`, `date_created`, and `tags`.
- [ ] All 11 sections from the template are present (even if marked N/A).
- [ ] File name matches the pattern `spec-[a-z0-9-]+.md`.
- [ ] File is saved in the workspace `/spec/` directory.
- [ ] Markdown is well-formed (no broken links, proper heading hierarchy).
- [ ] Requirements use the identifier format `XXX-NNN` (e.g., `REQ-001`).
- [ ] Acceptance criteria are testable and specific.

### 6. Present to the User

After creating the specification, provide the user with:
- A link to the created file.
- A brief summary of the specification's scope.
- Any open questions or areas that need further refinement.
