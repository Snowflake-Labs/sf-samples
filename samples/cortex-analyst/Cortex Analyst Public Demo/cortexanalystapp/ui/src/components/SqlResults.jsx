import React, { useRef, useEffect, useState } from "react";
import { motion } from "framer-motion";
import { FixedSizeList as List } from "react-window";
import AutoSizer from "react-virtualized-auto-sizer";

const ROW_HEIGHT = 40;
const MAX_HEIGHT = 400;
const SCROLLBAR_WIDTH = 17; // Default scrollbar width for most browsers

const SqlResults = ({ metadata, data }) => {
  if (!metadata?.columns || !data?.length) return null;
  const [isExpanded, setIsExpanded] = useState(true);
  const [hasVerticalScrollbar, setHasVerticalScrollbar] = useState(false);
  const [tooltip, setTooltip] = useState({
    show: false,
    content: "",
    x: 0,
    y: 0,
  });
  const listRef = useRef(null);

  useEffect(() => {
    setHasVerticalScrollbar(data.length * ROW_HEIGHT > MAX_HEIGHT);
  }, [data.length]);

  const getColumnWidth = (totalWidth) => {
    const adjustedWidth = hasVerticalScrollbar
      ? totalWidth - SCROLLBAR_WIDTH
      : totalWidth;
    return `${100 / metadata.columns.length}%`;
  };

  const handleCellMouseEnter = (event, cellContent) => {
    const rect = event.target.getBoundingClientRect();
    // Only show tooltip if content is truncated or if it's long
    if (
      cellContent &&
      (cellContent.toString().length > 20 ||
        event.target.scrollWidth > event.target.clientWidth)
    ) {
      setTooltip({
        show: true,
        content: cellContent.toString(),
        x: rect.left + rect.width / 2,
        y: rect.top - 8,
      });
    }
  };

  const handleCellMouseLeave = () => {
    setTooltip({ show: false, content: "", x: 0, y: 0 });
  };

  const handleCellTouch = (event, cellContent) => {
    event.preventDefault(); // Prevent default touch behavior
    const rect = event.target.getBoundingClientRect();

    // If the same cell is tapped again, hide the tooltip
    if (tooltip.show && tooltip.content === cellContent.toString()) {
      setTooltip({ show: false, content: "", x: 0, y: 0 });
      return;
    }

    // Only show tooltip if content is truncated or if it's long
    if (
      cellContent &&
      (cellContent.toString().length > 20 ||
        event.target.scrollWidth > event.target.clientWidth)
    ) {
      setTooltip({
        show: true,
        content: cellContent.toString(),
        x: rect.left + rect.width / 2,
        y: rect.top - 8,
      });
    }
  };

  // Handle clicking outside to dismiss tooltip on mobile
  useEffect(() => {
    const handleClickOutside = (event) => {
      // Only handle if tooltip is showing and it's not a cell click
      if (tooltip.show && !event.target.closest("[data-cell]")) {
        setTooltip({ show: false, content: "", x: 0, y: 0 });
      }
    };

    if (tooltip.show) {
      document.addEventListener("touchstart", handleClickOutside);
      document.addEventListener("click", handleClickOutside);
    }

    return () => {
      document.removeEventListener("touchstart", handleClickOutside);
      document.removeEventListener("click", handleClickOutside);
    };
  }, [tooltip.show]);

  const Row = ({ index, style }) => (
    <div
      style={style}
      className={`flex divide-x divide-gray-200 ${
        index % 2 === 0 ? "bg-white" : "bg-gray-50"
      }`}
    >
      {data[index].map((cell, cellIndex) => (
        <div
          key={`${index}-${cellIndex}`}
          className="px-6 py-2 whitespace-nowrap text-sm text-gray-900 flex-1 overflow-hidden text-ellipsis hover:bg-blue-50 cursor-default transition-colors duration-150"
          style={{ width: getColumnWidth() }}
          data-cell="true"
          onMouseEnter={(event) => handleCellMouseEnter(event, cell)}
          onMouseLeave={handleCellMouseLeave}
          onTouchStart={(event) => handleCellTouch(event, cell)}
        >
          {cell}
        </div>
      ))}
    </div>
  );

  return (
    <div className="mt-2 mb-4 rounded-lg border border-gray-200 w-3/4">
      <div
        className="bg-gray-200 px-4 py-2 flex justify-between items-center cursor-pointer hover:bg-gray-300"
        onClick={() => setIsExpanded(!isExpanded)}
      >
        <span className="text-sm font-mono text-gray-600">Query Results</span>
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
        initial={{ opacity: 0, y: 20 }}
        animate={{
          height: isExpanded ? "auto" : 0,
          opacity: isExpanded ? 1 : 0,
        }}
        className="w-full border border-gray-200 rounded-lg"
      >
        {/* Header */}
        <div className="bg-blue-100 border-b border-gray-200">
          <div
            className="flex divide-x divide-gray-200"
            style={{
              width: hasVerticalScrollbar
                ? `calc(100% - ${SCROLLBAR_WIDTH}px)`
                : "100%",
            }}
          >
            {metadata.columns.map((column) => (
              <div
                key={column.name}
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider flex-1 overflow-hidden text-ellipsis hover:bg-blue-200 cursor-default transition-colors duration-150"
                style={{ width: getColumnWidth() }}
                data-cell="true"
                onMouseEnter={(event) =>
                  handleCellMouseEnter(event, column.name)
                }
                onMouseLeave={handleCellMouseLeave}
                onTouchStart={(event) => handleCellTouch(event, column.name)}
              >
                {column.name}
              </div>
            ))}
          </div>
        </div>

        {/* Virtualized Body */}
        <div style={{ height: Math.min(data.length * ROW_HEIGHT, MAX_HEIGHT) }}>
          <AutoSizer>
            {({ height, width }) => (
              <List
                ref={listRef}
                height={height}
                itemCount={data.length}
                itemSize={ROW_HEIGHT}
                width={width}
              >
                {Row}
              </List>
            )}
          </AutoSizer>
        </div>

        {/* Footer with total count */}
        <div className="bg-gray-50 px-6 py-3 text-sm text-gray-500 border-t border-gray-200">
          {data.length !== metadata.totalRows ? (
            <span className="mr-2">
              Showing {data.length} rows of {metadata.totalRows}
            </span>
          ) : (
            <span className="mr-2">{data.length} rows</span>
          )}
        </div>
      </motion.div>

      {/* Custom Tooltip */}
      {tooltip.show && (
        <div
          className="fixed z-50 px-3 py-2 text-sm md:text-xs text-white bg-gray-800 rounded-lg shadow-lg pointer-events-none max-w-xs md:max-w-sm break-words"
          style={{
            left: tooltip.x,
            top: tooltip.y,
            transform: "translateX(-50%) translateY(-100%)",
          }}
        >
          {tooltip.content}
          {/* Mobile hint */}
          <div className="block md:hidden text-xs opacity-75 mt-1">
            Tap again to hide
          </div>
        </div>
      )}
    </div>
  );
};

export default SqlResults;
