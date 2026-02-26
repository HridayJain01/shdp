import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { Toaster } from "react-hot-toast";
import AppShell from "./layouts/AppShell";
import UploadPage from "./pages/UploadPage";
import PreviewPage from "./pages/PreviewPage";
import ProfilingPage from "./pages/ProfilingPage";
import AnomalyPage from "./pages/AnomalyPage";
import AIPlanPage from "./pages/AIPlanPage";
import QualityPage from "./pages/QualityPage";
import ComparisonPage from "./pages/ComparisonPage";
import HealthPage from "./pages/HealthPage";

export default function App() {
  return (
    <BrowserRouter>
      <Toaster
        position="top-right"
        toastOptions={{
          style: {
            borderRadius: "10px",
            background: "#1e293b",
            color: "#f1f5f9",
            fontSize: "13px",
          },
        }}
      />
      <Routes>
        <Route path="/" element={<AppShell />}>
          <Route index element={<Navigate to="/upload" replace />} />
          <Route path="upload"     element={<UploadPage />} />
          <Route path="preview"    element={<PreviewPage />} />
          <Route path="profile"    element={<ProfilingPage />} />
          <Route path="anomalies"  element={<AnomalyPage />} />
          <Route path="ai-plan"    element={<AIPlanPage />} />
          <Route path="quality"    element={<QualityPage />} />
          <Route path="comparison" element={<ComparisonPage />} />
          <Route path="health"     element={<HealthPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
