import React from "react";
import MessageContent from "./MessageContent";
import { motion } from "motion/react";
import StatusIndicator from "./StatusIndicator";
import SqlResults from "./SqlResults";

const Message = ({ role, content }) => {
  return (
    <div
      className={`mb-3 ${
        role === "user"
          ? "flex flex-col items-end"
          : "flex flex-col items-start"
      }`}
    >
      {role === "analyst" && (
        <span className="text-sm font-semibold text-gray-500 mb-2">
          Analyst
        </span>
      )}
      {content &&
        content.map((item, index) => {
          const hasSubsequentContent = index < content.length - 1;

          return item.metaType === "status" ? (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              key={item.status}
            >
              <StatusIndicator
                status={item.status}
                isLatest={index === content.length - 1}
              />
            </motion.div>
          ) : item.metaType == "sql_results" ? (
            <SqlResults key="sql" metadata={item.metadata} data={item.data} />
          ) : (
            <MessageContent
              key={JSON.stringify(item)}
              content={item}
              role={role}
              hasSubsequentContent={hasSubsequentContent}
            />
          );
        })}
    </div>
  );
};

export default Message;
