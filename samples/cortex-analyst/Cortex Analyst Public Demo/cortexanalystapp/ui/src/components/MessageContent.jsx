import React, { useEffect, useState } from "react";
import Prism from "prismjs";
import "prismjs/themes/prism-tomorrow.css";
import "prismjs/components/prism-sql";
import { motion } from "motion/react";
import SqlResults from "./SqlResults";

function MessageContent({ content, role, hasSubsequentContent }) {
  const [isExpanded, setIsExpanded] = useState(true);

  useEffect(() => {
    Prism.highlightAll();
  }, [content, isExpanded]);

  // Auto-collapse SQL queries when subsequent content is added
  useEffect(() => {
    if (content.type === "sql" && hasSubsequentContent) {
      setIsExpanded(false);
    }
  }, [hasSubsequentContent, content.type]);

  switch (content.type) {
    case "error":
      return (
        <div className="mt-2 rounded-lg overflow-hidden border border-red-200 bg-red-50 w-3/4">
          <div className="p-4">
            <div className="flex items-start gap-3">
              <div className="flex-shrink-0">
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
                  className="text-red-500"
                >
                  <circle cx="12" cy="12" r="10" />
                  <line x1="15" y1="9" x2="9" y2="15" />
                  <line x1="9" y1="9" x2="15" y2="15" />
                </svg>
              </div>
              <div className="flex-1">
                <h4 className="text-sm font-medium text-red-800 mb-1">
                  Error occurred
                </h4>
                <p className="text-sm text-red-700 mb-3">{content.text}</p>
                {content.onRetry && (
                  <motion.button
                    whileHover={{ scale: 1.02 }}
                    whileTap={{ scale: 0.98 }}
                    onClick={content.onRetry}
                    className="inline-flex items-center gap-2 px-3 py-2 text-sm font-medium text-red-700 bg-red-100 hover:bg-red-200 rounded-md border border-red-300 transition-colors"
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
                      <path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8" />
                      <path d="M21 3v5h-5" />
                      <path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16" />
                      <path d="M3 21v-5h5" />
                    </svg>
                    Retry
                  </motion.button>
                )}
              </div>
            </div>
          </div>
        </div>
      );
    case "sql":
      return (
        <div className="mt-2 rounded-lg overflow-hidden border border-gray-200 w-3/4">
          <div
            className="bg-gray-200 px-4 py-2 flex justify-between items-center cursor-pointer hover:bg-gray-300"
            onClick={() => setIsExpanded(!isExpanded)}
          >
            <span className="text-sm font-mono text-gray-600">SQL Query</span>
            <motion.svg
              xmlns="http://www.w3.org/2000/svg"
              width="16"
              height="16"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
              animate={{ rotate: isExpanded ? 180 : 0 }}
              className="text-gray-500"
            >
              <path d="M19 9l-7 7-7-7" />
            </motion.svg>
          </div>
          <motion.div
            initial={false}
            animate={{
              height: isExpanded ? "auto" : 0,
              opacity: isExpanded ? 1 : 0,
            }}
            transition={{ duration: 0.2 }}
            className="overflow-hidden"
          >
            <pre className="m-0 p-4 bg-gray-900 overflow-x-auto">
              <code className="language-sql">{content.statement}</code>
            </pre>
          </motion.div>
        </div>
      );
    case "text":
      // Text formatting for regular content
      const formatText = (text) => {
        return text.split("\n\n").map((line, i) => {
          line = line.trim();
          const shouldBeBold = line.startsWith("__") && line.endsWith("__");
          const cleanText = shouldBeBold ? line.slice(2, -2) : line;
          return (
            <React.Fragment key={i}>
              {i > 0 && <br />}
              {shouldBeBold ? (
                <span key={cleanText} className="font-bold">
                  {cleanText}
                </span>
              ) : (
                cleanText
              )}
            </React.Fragment>
          );
        });
      };

      return (
        <span
          className={`inline-block px-4 py-3 max-w-[60%] ${
            role === "user"
              ? "bg-sky-600 text-white rounded-xl rounded-br-sm"
              : "bg-gray-100 rounded-xl text-gray-900"
          }`}
        >
          {formatText(content.text)}
        </span>
      );
    case "sql_results":
      return <SqlResults metadata={content.metadata} data={content.data} />;
  }
  return null;
}

export default MessageContent;
