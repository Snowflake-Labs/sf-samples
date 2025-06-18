import React, { useEffect, useState } from "react";
import Message from "./Message";
import { AnimatePresence, motion } from "motion/react";
import ApiService from "../ApiService";
import { compressMessages } from "../utils/compression";

const updateSQLResultsInContentHistory = (
  partialContentHistory,
  metadata,
  data
) => {
  let contentIndexInArray = -1;
  // Find the index of the content in the partial content history
  for (let i = partialContentHistory.length - 1; i >= 0; i--) {
    if (partialContentHistory[i].metaType === "sql_results") {
      contentIndexInArray = i;
      break;
    }
  }
  const sqlMessage =
    contentIndexInArray === -1
      ? { metaType: "sql_results", data: [] }
      : structuredClone(partialContentHistory[contentIndexInArray]);
  if (metadata) {
    sqlMessage.metadata = metadata;
  }
  if (data) {
    sqlMessage.data.push(...data);
  }
  if (contentIndexInArray === -1) {
    return [...partialContentHistory, sqlMessage];
  }
  return [
    ...partialContentHistory.slice(0, contentIndexInArray),
    sqlMessage,
    ...partialContentHistory.slice(contentIndexInArray + 1),
  ];
};

const updatePartialContentHistory = (partialContentHistory, content) => {
  let contentIndexInArray = -1;
  // Find the index of the content in the partial content history
  for (let i = partialContentHistory.length - 1; i >= 0; i--) {
    if (partialContentHistory[i].metaType === "content") {
      if (partialContentHistory[i].index === content.index) {
        contentIndexInArray = i;
        break;
      }
    }
  }
  const currentMessage =
    contentIndexInArray === -1
      ? { metaType: "content", index: content.index, type: content.type }
      : structuredClone(partialContentHistory[contentIndexInArray]);

  switch (content.type) {
    case "text":
      currentMessage.text = (currentMessage.text || "") + content.text_delta;
      break;
    case "suggestions":
      if (!currentMessage.suggestions) {
        currentMessage.suggestions = [];
      }
      if (content.suggestions_delta?.suggestion_delta) {
        currentMessage.suggestions[content.suggestions_delta.index] =
          (currentMessage.suggestions[content.suggestions_delta.index] || "") +
          content.suggestions_delta.suggestion_delta;
      }
      break;
    case "sql":
      currentMessage.statement =
        (currentMessage.statement || "") + content.statement_delta;
      break;
    default:
      return partialContentHistory;
  }
  if (contentIndexInArray === -1) {
    return [...partialContentHistory, currentMessage];
  }
  return [
    ...partialContentHistory.slice(0, contentIndexInArray),
    currentMessage,
    ...partialContentHistory.slice(contentIndexInArray + 1),
  ];
};

const ChatContainer = ({ model, onReset, initialMessages }) => {
  const [messages, setMessages] = useState([
    {
      content: [{ type: "text", text: "Hello! How can I help you today?" }],
      role: "analyst",
    },
  ]);
  const [generating, setGenerating] = useState(false);
  const [partialContentHistory, setPartialContentHistory] = useState([]);
  const [lastMessageSuggestions, setLastMessageSuggestions] = useState([]);
  const [input, setInput] = useState("");
  const [shareModalOpen, setShareModalOpen] = useState(false);
  const [shareUrl, setShareUrl] = useState("");
  const [shareError, setShareError] = useState("");
  const messageQueueRef = React.useRef(0);
  const contentPartialHistoryRef = React.useRef([]);
  const messagesContainerRef = React.useRef(null);

  // Smooth scroll to bottom function
  const scrollToBottom = (smooth = true) => {
    if (messagesContainerRef.current) {
      messagesContainerRef.current.scrollTo({
        top: messagesContainerRef.current.scrollHeight,
        behavior: smooth ? "smooth" : "auto",
      });
    }
  };

  // Initialize messages from prop if provided
  useEffect(() => {
    if (
      initialMessages &&
      Array.isArray(initialMessages) &&
      initialMessages.length > 0
    ) {
      setMessages(initialMessages);
    }
  }, [initialMessages]);

  const handleSend = (text) => {
    if (text.trim() === "") return;

    if (generating) return;

    // Add user message
    setMessages((prev) => [
      ...prev,
      { content: [{ type: "text", text }], role: "user" },
    ]);
    setInput("");
  };

  const retryLastMessage = () => {
    if (generating) return;

    // Remove the last error message if it exists
    setMessages((prev) => {
      const filtered = prev.filter((msg) => !msg.error);
      return filtered;
    });

    // Find the last user message to re-send
    const userMessages = messages.filter((msg) => msg.role === "user");
    if (userMessages.length > 0) {
      const lastUserText =
        userMessages[userMessages.length - 1].content[0].text;
      // Re-trigger the streaming by updating messages to trigger useEffect
      setMessages((prev) => {
        const withoutErrors = prev.filter((msg) => !msg.error);
        // Add a dummy update to trigger useEffect
        return [...withoutErrors];
      });
    }
  };

  const shareConversation = async () => {
    try {
      const compressed = compressMessages(messages);
      const url = `${window.location.origin}${window.location.pathname}?m=${encodeURIComponent(model.Name)}&h=${compressed}`;

      // Check if URL is too long (30,000 character limit)
      if (url.length > 30_000) {
        setShareError(
          "Unfortunately, this conversation is too long to be shared via URL. The compressed URL would exceed 30,000 characters. Consider sharing a shorter conversation or specific parts of it."
        );
        setShareUrl("");
      } else {
        setShareError("");
        setShareUrl(url);
      }
    } catch (error) {
      console.error("Error generating share URL:", error);
      setShareError("Failed to generate share URL. Please try again.");
      setShareUrl("");
    }
  };

  const copyToClipboard = async () => {
    try {
      await navigator.clipboard.writeText(shareUrl);

      // Show success feedback
      const button = document.querySelector("#copy-url-button");
      if (button) {
        const originalText = button.textContent;
        button.textContent = "✓ Copied!";
        setTimeout(() => {
          button.textContent = originalText;
        }, 2000);
      }
    } catch (error) {
      console.error("Error copying to clipboard:", error);
      alert("Failed to copy URL. Please try again.");
    }
  };

  // Generate share URL when modal opens
  useEffect(() => {
    if (shareModalOpen) {
      shareConversation();
    }
  }, [shareModalOpen]);

  useEffect(() => {
    if (messages[messages.length - 1].role !== "user") return;
    if (generating) return;
    contentPartialHistoryRef.current = [
      {
        status: "Sending request",
        metaType: "status",
      },
    ];
    // Start streaming response
    setGenerating(true);
    setPartialContentHistory(() => contentPartialHistoryRef.current);
    ApiService.streamChat(
      model.Name,
      messages.slice(1),
      (message) => {
        messageQueueRef.current++;
        setPartialContentHistory((prev) => {
          contentPartialHistoryRef.current = updatePartialContentHistory(
            prev,
            message
          );
          messageQueueRef.current--;
          return contentPartialHistoryRef.current;
        });
      },
      (error) => {
        console.error("Streaming error:", error);
        setGenerating(false);
        setPartialContentHistory([]);

        // Determine error message
        let errorMessage = "Sorry, there was an error processing your request.";
        if (error.message) {
          errorMessage = error.message;
        } else if (typeof error === "string") {
          errorMessage = error;
        } else if (error.toString && error.toString() !== "[object Object]") {
          errorMessage = error.toString();
        }

        setMessages((prev) => [
          ...prev,
          {
            content: [
              {
                type: "error",
                text: errorMessage,
                onRetry: retryLastMessage,
              },
            ],
            role: "analyst",
            error: true,
          },
        ]);
      },
      (status) => {
        messageQueueRef.current++;
        setPartialContentHistory((prev) => {
          contentPartialHistoryRef.current = [
            ...prev,
            { status, metaType: "status" },
          ];
          messageQueueRef.current--;
          return contentPartialHistoryRef.current;
        });
      },
      (sqlExecResult) => {
        messageQueueRef.current++;
        setPartialContentHistory((prev) => {
          contentPartialHistoryRef.current = updateSQLResultsInContentHistory(
            prev,
            sqlExecResult.metadata,
            sqlExecResult.data
          );
          messageQueueRef.current--;
          return contentPartialHistoryRef.current;
        });
      },
      () => {
        // Wait for all messages to be processed
        const checkQueue = () => {
          if (messageQueueRef.current > 0) {
            setTimeout(checkQueue, 50); // Check again in 50ms
            return;
          }

          if (contentPartialHistoryRef.current.length === 0) {
            return;
          }

          // All messages processed, now we can update state
          setMessages((prev) => [
            ...prev,
            {
              fullContent: contentPartialHistoryRef.current.filter(
                (content) =>
                  content.metaType === "content" ||
                  content.metaType === "sql_results"
              ),
              content: contentPartialHistoryRef.current.filter(
                (content) => content.metaType === "content"
              ),
              role: "analyst",
            },
          ]);

          setLastMessageSuggestions(
            contentPartialHistoryRef.current
              .filter(
                (content) =>
                  content.metaType === "content" &&
                  content.type === "suggestions"
              )
              .flatMap((content) =>
                content.suggestions.filter(
                  (suggestion) => suggestion && suggestion.trim() !== ""
                )
              )
          );
          setGenerating(false);
          setPartialContentHistory([]);
          contentPartialHistoryRef.current = [];
        };

        // HACK: small delay to prevent race condition
        setTimeout(checkQueue, 100);
      }
    );
  }, [model.Name, messages]);

  const [suggestedQuestions, setSuggestedQuestions] = useState([]);
  useEffect(() => {
    const hasUserMessages = messages.some((message) => message.role === "user");
    if (input === "" && !generating) {
      if (hasUserMessages && messages[messages.length - 1].role !== "user") {
        setSuggestedQuestions(lastMessageSuggestions);
      } else {
        setSuggestedQuestions(model.SuggestedQuestions);
      }
    } else {
      setSuggestedQuestions([]);
    }
  }, [
    input,
    messages,
    generating,
    lastMessageSuggestions,
    model.SuggestedQuestions,
  ]);

  // Auto-scroll when messages change (new messages added)
  useEffect(() => {
    // Small delay to ensure DOM has updated
    const timeoutId = setTimeout(() => {
      scrollToBottom(true);
    }, 100);

    return () => clearTimeout(timeoutId);
  }, [messages]);

  // Auto-scroll when partial content changes (during streaming)
  useEffect(() => {
    if (generating && partialContentHistory.length > 0) {
      // More frequent scrolling during generation, but with shorter delay
      const timeoutId = setTimeout(() => {
        scrollToBottom(true);
      }, 50);

      return () => clearTimeout(timeoutId);
    }
  }, [partialContentHistory, generating]);

  // Auto-scroll when loading initial messages from URL (smooth)
  useEffect(() => {
    if (
      initialMessages &&
      Array.isArray(initialMessages) &&
      initialMessages.length > 0
    ) {
      // Longer delay for initial load to ensure all components are rendered
      const timeoutId = setTimeout(() => {
        scrollToBottom(true);
      }, 500);

      return () => clearTimeout(timeoutId);
    }
  }, [initialMessages]);

  return (
    <div className="flex-1 flex flex-col py-8 overflow-hidden bg-white w-full">
      {/* Model Info */}
      <div className="mb-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <motion.button
            onClick={onReset}
            className="p-2 hover:bg-gray-100 rounded-full transition-colors"
            whileHover={{ scale: 1.1 }}
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              width="24"
              height="24"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
              className="text-gray-600"
            >
              <path d="M19 12H5M12 19l-7-7 7-7" />
            </svg>
          </motion.button>
          <div>
            <h2 className="text-lg font-semibold text-gray-900">
              {model.Name}
            </h2>
            <p className="text-sm text-gray-600">{model.Description}</p>
          </div>
        </div>

        {/* Share button - only show if there are user messages */}
        {messages.some((msg) => msg.role === "user") && !generating && (
          <motion.button
            onClick={() => setShareModalOpen(true)}
            className="flex items-center gap-2 px-3 py-2 text-sm font-medium text-blue-700 bg-blue-100 hover:bg-blue-200 rounded-lg border border-blue-300 transition-colors"
            whileHover={{ scale: 1.02 }}
            whileTap={{ scale: 0.98 }}
            title="Share this conversation"
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              width="16"
              height="16"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <path d="M4 12v8a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-8" />
              <polyline points="16,6 12,2 8,6" />
              <line x1="12" y1="2" x2="12" y2="15" />
            </svg>
            Share
          </motion.button>
        )}
      </div>
      <motion.div
        initial={{ opacity: 0.1 }}
        animate={{ opacity: 1, transition: { duration: 1 } }}
        className="flex-1 overflow-y-auto mb-4 p-6 bg-gray-50 rounded-lg"
        ref={messagesContainerRef}
      >
        {messages.map((message) => (
          <Message
            key={JSON.stringify(message)}
            role={message.role}
            content={message.fullContent || message.content}
          />
        ))}
        {generating && (
          <Message
            key={"generating"}
            role="analyst"
            content={partialContentHistory}
          />
        )}
        {suggestedQuestions.length > 0 && (
          <AnimatePresence>
            <motion.div
              initial={{ opacity: 0, y: -10 }}
              animate={{ opacity: 1, y: 0, transition: { duration: 2 } }}
              exit={{ opacity: 0 }}
              className="flex flex-col gap-2 items-end"
            >
              {suggestedQuestions.map((question) => (
                <div className="flex items-center gap-2" key={question}>
                  <motion.button
                    whileHover={{ scale: 1.05 }}
                    onClick={() => setInput(question)}
                    className="p-2 hover:bg-blue-200 rounded-lg transition-colors"
                    title="Fill in input"
                  >
                    <motion.svg
                      xmlns="http://www.w3.org/2000/svg"
                      width="20"
                      height="20"
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="2"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      className="text-blue-500"
                      whileHover={{ y: 2 }}
                      transition={{ type: "spring", stiffness: 300 }}
                    >
                      <path d="M12 5v14M19 12l-7 7-7-7" />
                    </motion.svg>
                  </motion.button>
                  <motion.button
                    whileHover={{ scale: 1.05 }}
                    onClick={() => handleSend(question)}
                    className="flex-1 text-left px-4 py-2 rounded-lg border-blue-200 border-4 hover:border-blue-300 hover:bg-blue-200 transition-colors"
                  >
                    {question}
                  </motion.button>
                </div>
              ))}
            </motion.div>
          </AnimatePresence>
        )}
      </motion.div>

      {/* Input Container */}
      <motion.div
        initial={{ opacity: 0.1 }}
        animate={{ opacity: 1, transition: { duration: 1 } }}
        className="flex items-center bg-white p-4"
      >
        <input
          type="text"
          className="flex-1 px-4 py-3 text-sm border border-gray-200 rounded-lg focus:outline-none focus:border-primary focus:ring-2 focus:ring-primary/10"
          placeholder="Type your message..."
          value={input}
          onKeyDown={(e) => e.key === "Enter" && handleSend(input)}
          onChange={(e) => setInput(e.target.value)}
        />
        <button
          className="ml-3 px-5 py-3 bg-blue-400 text-white rounded-lg hover:bg-blue-500 transition-colors"
          onClick={() => handleSend(input)}
        >
          Send
        </button>
      </motion.div>

      {/* Share Modal */}
      {shareModalOpen && (
        <div className="fixed inset-0 backdrop-blur-xs bg-opacity-50 flex items-center justify-center z-50">
          <motion.div
            initial={{ opacity: 0, scale: 0.9 }}
            animate={{ opacity: 1, scale: 1 }}
            exit={{ opacity: 0, scale: 0.9 }}
            className="bg-white rounded-lg p-6 w-full max-w-2xl mx-4 max-h-[80vh] overflow-y-auto"
          >
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-semibold text-gray-900">
                Share Conversation
              </h3>
              <button
                onClick={() => setShareModalOpen(false)}
                className="p-2 hover:bg-gray-100 rounded-full transition-colors"
              >
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
                  className="text-gray-600"
                >
                  <path d="M18 6L6 18M6 6l12 12" />
                </svg>
              </button>
            </div>

            {/* Privacy Warning */}
            {!shareError && (
              <div className="mb-4 p-4 bg-amber-50 border border-amber-200 rounded-lg">
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
                    className="text-amber-600 mt-0.5 flex-shrink-0"
                  >
                    <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" />
                    <line x1="12" y1="9" x2="12" y2="13" />
                    <line x1="12" y1="17" x2="12.01" y2="17" />
                  </svg>
                  <div>
                    <h4 className="font-medium text-amber-800 mb-1">
                      Privacy Notice
                    </h4>
                    <p className="text-sm text-amber-700">
                      <strong>
                        The entire conversation history is stored in this URL.
                      </strong>{" "}
                      Only share this link with people you trust. Anyone with
                      access to this URL will be able to see all messages in
                      this conversation.
                    </p>
                  </div>
                </div>
              </div>
            )}

            {shareError ? (
              <div className="mb-4 p-4 bg-red-50 border border-red-200 rounded-lg">
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
                    className="text-red-600 mt-0.5 flex-shrink-0"
                  >
                    <circle cx="12" cy="12" r="10" />
                    <line x1="15" y1="9" x2="9" y2="15" />
                    <line x1="9" y1="9" x2="15" y2="15" />
                  </svg>
                  <div>
                    <h4 className="font-medium text-red-800 mb-1">
                      Unable to Share
                    </h4>
                    <p className="text-sm text-red-700">{shareError}</p>
                  </div>
                </div>
              </div>
            ) : (
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Share URL
                  </label>
                  <div className="flex gap-2">
                    <input
                      type="text"
                      value={shareUrl}
                      readOnly
                      className="flex-1 px-3 py-2 border border-gray-300 rounded-lg bg-gray-50 text-sm font-mono text-gray-700 overflow-ellipsis focus:outline-none"
                      placeholder="Generating URL..."
                    />
                    <button
                      id="copy-url-button"
                      onClick={copyToClipboard}
                      disabled={!shareUrl}
                      className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors text-sm font-medium"
                    >
                      Copy
                    </button>
                  </div>
                </div>

                {shareUrl && (
                  <div className="text-sm text-gray-600">
                    <p>
                      URL length: {shareUrl.length.toLocaleString()} characters
                    </p>
                  </div>
                )}
              </div>
            )}

            <div className="flex justify-end gap-3 mt-6">
              <button
                onClick={() => setShareModalOpen(false)}
                className="px-4 py-2 text-gray-700 bg-gray-100 hover:bg-gray-200 rounded-lg transition-colors"
              >
                Close
              </button>
            </div>
          </motion.div>
        </div>
      )}
    </div>
  );
};

export default ChatContainer;
