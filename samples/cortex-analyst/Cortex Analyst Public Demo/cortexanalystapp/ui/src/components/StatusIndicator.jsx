import React from "react";
import { motion, AnimatePresence } from "motion/react";

const StatusIndicator = ({ status, isLatest }) => {
  return (
    <motion.div
      className="flex items-center gap-3 text-gray-500 my-4"
      exit={{ opacity: 0, transition: { duration: 5 } }}
    >
      <AnimatePresence mode="wait">
        {isLatest ? (
          <motion.div
            key="spinner"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
          >
            <motion.div
              animate={{ rotate: 360 }}
              transition={{ duration: 1, repeat: Infinity, ease: "linear" }}
              className="w-5 h-5 border-2 border-blue-500 border-t-transparent rounded-full"
            />
          </motion.div>
        ) : (
          <motion.div
            key="checkmark"
            initial={{ scale: 0.5, opacity: 0 }}
            animate={{ scale: 1, opacity: 1 }}
            transition={{ type: "spring", duration: 1 }}
            className="w-5 h-5 flex items-center justify-center rounded-full bg-green-500 text-white"
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="h-3 w-3"
              viewBox="0 0 20 20"
              fill="currentColor"
            >
              <path
                fillRule="evenodd"
                d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z"
                clipRule="evenodd"
              />
            </svg>
          </motion.div>
        )}
      </AnimatePresence>
      <span className="text-sm capitalize">
        {status.replace(/_/g, " ")}
        {isLatest ? "..." : ""}
      </span>
    </motion.div>
  );
};

export default StatusIndicator;
