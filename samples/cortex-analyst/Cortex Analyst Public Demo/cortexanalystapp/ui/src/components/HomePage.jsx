import React from "react";
import { motion } from "motion/react";

const HomePage = ({ models, onModelSelect }) => {
  const [selectedModel, setSelectedModel] = React.useState(null);

  return (
    <div className="flex flex-col items-center justify-center max-w-2xl mx-auto mt-12 p-8">
      <h1 className="text-4xl font-bold text-gray-900 mb-6">
        Welcome to Cortex Analyst
      </h1>

      <p className="text-lg text-gray-600 mb-8 text-center">
        This demo showcases natural language querying capabilities across
        different datasets. Select a model below to begin exploring.
      </p>

      <div className="w-full space-y-4">
        {models.map((model) => (
          <motion.button
            whileHover={{ scale: 1.05 }}
            key={model.Name}
            className={`w-full p-4 rounded-lg border transition-all ${
              selectedModel?.Name === model.Name
                ? "border-blue-500 bg-blue-50"
                : "border-gray-200 hover:border-blue-300 hover:bg-blue-100"
            }`}
            onClick={() => {
              setSelectedModel(model);
              onModelSelect(model);
            }}
          >
            <h3 className="text-lg font-semibold text-gray-900">
              {model.Name}
            </h3>
            <p className="text-gray-600 mt-2">{model.Description}</p>
          </motion.button>
        ))}
      </div>
    </div>
  );
};

export default HomePage;
