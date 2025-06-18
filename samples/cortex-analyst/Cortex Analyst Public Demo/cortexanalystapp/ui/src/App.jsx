import React, { useState, useEffect } from "react";
import SnowflakeLogo from "./assets/bug-R-sno-blue.svg";
import ChatContainer from "./components/ChatContainer";
import HomePage from "./components/HomePage";
import "./App.css";
import ApiService from "./ApiService";
import { decompressMessages } from "./utils/compression";

function App() {
  const [models, setModels] = useState([]);
  const [modelError, setModelError] = useState(null);
  const [selectedModel, setSelectedModel] = useState(null);
  const [initialMessages, setInitialMessages] = useState(null);
  const [urlError, setUrlError] = useState(null);

  useEffect(() => {
    const fetchModels = async () => {
      try {
        const models = await ApiService.getModels();
        setModels(models);

        // After models are loaded, check URL parameters
        checkUrlParameters(models);
      } catch (error) {
        setModelError(error);
      }
    };

    fetchModels();
  }, []);

  const checkUrlParameters = (availableModels) => {
    try {
      const urlParams = new URLSearchParams(window.location.search);
      const modelParam = urlParams.get("m");
      const historyParam = urlParams.get("h");

      if (modelParam && historyParam) {
        // Find the model
        const model = availableModels.find((m) => m.Name === modelParam);
        if (!model) {
          setUrlError(
            `Model "${modelParam}" not found. Available models: ${availableModels.map((m) => m.Name).join(", ")}`
          );
          return;
        }

        // Decompress the history
        const messages = decompressMessages(historyParam);

        // Validate messages structure
        if (!Array.isArray(messages) || messages.length === 0) {
          setUrlError("Invalid message history format");
          return;
        }

        // Set the model and messages
        setSelectedModel(model);
        setInitialMessages(messages);

        // Clear URL parameters after loading (optional)
        window.history.replaceState(
          {},
          document.title,
          window.location.pathname
        );
      }
    } catch (error) {
      console.error("Error loading from URL parameters:", error);
      setUrlError(`Failed to load chat from URL: ${error.message}`);
    }
  };

  return (
    <div className="flex flex-col h-screen max-w-6xl mx-auto w-full px-8">
      {/* Header */}
      <header className="bg-white text-gray-900 py-4 px-8 text-3xl font-semibold border-b border-gray-200 flex items-center gap-4">
        <img src={SnowflakeLogo} alt="Snowflake Logo" className="h-10 w-10" />
        Cortex Analyst
      </header>

      {/* Model Error */}
      {modelError && (
        <div className="bg-red-100 text-red-900 p-4 mt-4 rounded-lg">
          Failed to fetch models: {modelError.message}. Try refreshing the page.
        </div>
      )}

      {/* URL Error */}
      {urlError && (
        <div className="bg-orange-100 text-orange-900 p-4 mt-4 rounded-lg">
          <div className="flex items-start gap-3">
            <svg
              xmlns="http://www.w3.org/2000/svg"
              width="20"
              height="20"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
              className="flex-shrink-0 mt-0.5"
            >
              <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" />
              <line x1="12" y1="9" x2="12" y2="13" />
              <line x1="12" y1="17" x2="12.01" y2="17" />
            </svg>
            <div>
              <strong>URL Loading Error:</strong> {urlError}
            </div>
          </div>
        </div>
      )}

      {/* Show HomePage or ChatContainer based on model selection */}
      {!selectedModel ? (
        <HomePage models={models} onModelSelect={setSelectedModel} />
      ) : (
        <ChatContainer
          model={selectedModel}
          onReset={() => {
            setSelectedModel(null);
            setInitialMessages(null);
            setUrlError(null);
          }}
          initialMessages={initialMessages}
        />
      )}
    </div>
  );
}

export default App;
