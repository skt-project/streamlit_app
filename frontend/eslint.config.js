// Minimal ESLint config — its primary job is enforcing the Rules of Hooks.
// A hooks-after-early-return bug shipped to production as React error #310
// (VisitDetail); react-hooks/rules-of-hooks makes that class of bug a lint error.
import tseslint from "typescript-eslint";
import reactHooks from "eslint-plugin-react-hooks";

export default tseslint.config(
  {
    files: ["src/**/*.{ts,tsx}"],
    extends: [tseslint.configs.base],
    plugins: { "react-hooks": reactHooks },
    rules: {
      "react-hooks/rules-of-hooks": "error",
      "react-hooks/exhaustive-deps": "warn",
    },
  },
);
