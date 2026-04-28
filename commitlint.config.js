// commitlint.config.js
module.exports = {
  extends: ["@commitlint/config-conventional"],
  rules: {
    // Tipos permitidos no projeto
    "type-enum": [
      2,
      "always",
      ["feat", "fix", "docs", "style", "refactor", "perf", "test", "chore", "ci", "build"],
    ],
    // Escopos permitidos (opcional — remova se quiser escopos livres)
    "scope-enum": [
      2,
      "always",
      ["ingest", "staging", "gold", "dashboard", "docs", "ci", "deps", "config"],
    ],
    // Máximo de 72 caracteres no título
    "header-max-length": [2, "always", 72],
    // Descrição em minúsculas
    "subject-case": [2, "always", "lower-case"],
    // Não terminar com ponto
    "subject-full-stop": [2, "never", "."],
  },
};
