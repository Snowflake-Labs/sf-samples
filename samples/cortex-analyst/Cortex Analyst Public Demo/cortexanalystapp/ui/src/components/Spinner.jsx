import React from "react";
import { motion } from "motion/react";

const Spinner = ({ status }) => {
  return (
    <div className="flex items-center gap-3 text-gray-500 my-4">
      <motion.div
        animate={{ rotate: 360 }}
        transition={{ duration: 1, repeat: Infinity, ease: "linear" }}
        className="w-5 h-5 border-2 border-blue-500 border-t-transparent rounded-full"
      />
      <span className="text-sm capitalize">{status.replace(/_/g, " ")}...</span>
    </div>
  );
};

export default Spinner;
