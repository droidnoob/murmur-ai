import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { MurmurDashboard } from "./dashboard";
import "./tokens.css";
import "./app.css";

const root = document.getElementById("root");
if (!root) throw new Error("#root not found");

createRoot(root).render(
  <StrictMode>
    <MurmurDashboard />
  </StrictMode>,
);
