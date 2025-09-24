import "react-tooltip/dist/react-tooltip.css";
import React, { HTMLProps } from "react";
import { Tooltip } from "react-tooltip";
import * as Tabs from "@radix-ui/react-tabs";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { oneDark } from "react-syntax-highlighter/dist/esm/styles/prism";
import { LoadingSpinner } from "@/components/LoadingSpinner";
import { ApiState, SqlResponse } from "@/src/useApi";
import { useLocalStorage } from "@/src/useLocalStorage";

export interface ResultVisualizerProps extends HTMLProps<HTMLDivElement> {
  state: ApiState;
}

export function ResultVisualizer({
  state,
  className = "",
  ...rest
}: ResultVisualizerProps) {
  if (state.type === "nothing") return null;

  return (
    <div className={`${className} relative`} {...rest}>
      <Tooltip id="cell-tooltip" delayShow={500} />
      <div className="h-full overflow-auto -mt-0.5">
        {state.type === "loading" && <LoadingSpinner />}
        {state.type === "error" && (
          <div className="flex items-center justify-center h-full text-text-error text-center text-xl mx-12">
            {state.message}
          </div>
        )}
        {state.type === "result" && (
          <SqlResponseVisualizer result={state.result} />
        )}
      </div>
    </div>
  );
}

const SqlResponseVisualizer = React.memo(
  ({ result }: { result: SqlResponse }) => {
    const [selectedTab, setSelectedTab] = useLocalStorage(
      "result-tab",
      "table",
    );
    const [isFullscreen, setIsFullscreen] = React.useState(false);
    const tabContentRef = React.useRef<HTMLDivElement>(null);
    const fullscreenContentRef = React.useRef<HTMLDivElement>(null);
    const [buttonPosition, setButtonPosition] = React.useState({ top: 0, right: 0 });

    React.useEffect(() => {
      const updateButtonPosition = () => {
        if (tabContentRef.current && selectedTab === "graphviz_svg") {
          const rect = tabContentRef.current.getBoundingClientRect();
          setButtonPosition({
            top: rect.top + 8,
            right: window.innerWidth - rect.right + 8,
          });
        }
      };

      // Update position immediately when tab changes
      if (selectedTab === "graphviz_svg") {
        // Small delay to ensure DOM is updated
        setTimeout(updateButtonPosition, 0);
      }

      window.addEventListener('resize', updateButtonPosition);
      window.addEventListener('scroll', updateButtonPosition, true);

      return () => {
        window.removeEventListener('resize', updateButtonPosition);
        window.removeEventListener('scroll', updateButtonPosition, true);
      };
    }, [selectedTab]);

    // Center content when toggling fullscreen
    React.useEffect(() => {
      const centerContent = () => {
        const targetRef = isFullscreen ? fullscreenContentRef : tabContentRef;
        if (targetRef.current) {
          const container = targetRef.current;
          const svg = container.querySelector('svg');
          if (svg) {
            const svgRect = svg.getBoundingClientRect();
            const containerRect = container.getBoundingClientRect();

            // Center horizontally
            const horizontalScroll = Math.max(0, (svg.scrollWidth - containerRect.width) / 2);
            container.scrollLeft = horizontalScroll;

            // Scroll to top
            container.scrollTop = 0;
          }
        }
      };

      if (selectedTab === "graphviz_svg") {
        // Small delay to ensure DOM is updated
        setTimeout(centerContent, 0);
      }
    }, [isFullscreen, selectedTab]);

    const toggleFullscreen = () => {
      setIsFullscreen(!isFullscreen);
    };

    return (
      <Tabs.Root
        value={selectedTab}
        onValueChange={setSelectedTab}
        className="h-full flex flex-col"
      >
        <Tabs.List className="flex bg-primary-surface border-b border-border mb-2">
          <Tabs.Trigger
            value="table"
            className="px-4 py-2 font-medium text-sm border-b-2 border-transparent text-text-secondary hover:text-text-primary hover:bg-secondary-surface transition-colors data-[state=active]:border-accent data-[state=active]:text-text-primary data-[state=active]:bg-secondary-surface cursor-pointer"
          >
            Table
          </Tabs.Trigger>
          <Tabs.Trigger
            value="logical"
            className="px-4 py-2 font-medium text-sm border-b-2 border-transparent text-text-secondary hover:text-text-primary hover:bg-secondary-surface transition-colors data-[state=active]:border-accent data-[state=active]:text-text-primary data-[state=active]:bg-secondary-surface cursor-pointer"
          >
            Logical Plan
          </Tabs.Trigger>
          <Tabs.Trigger
            value="physical"
            className="px-4 py-2 font-medium text-sm border-b-2 border-transparent text-text-secondary hover:text-text-primary hover:bg-secondary-surface transition-colors data-[state=active]:border-accent data-[state=active]:text-text-primary data-[state=active]:bg-secondary-surface cursor-pointer"
          >
            Physical Plan
          </Tabs.Trigger>
          <Tabs.Trigger
            value="graphviz_svg"
            className="px-4 py-2 font-medium text-sm border-b-2 border-transparent text-text-secondary hover:text-text-primary hover:bg-secondary-surface transition-colors data-[state=active]:border-accent data-[state=active]:text-text-primary data-[state=active]:bg-secondary-surface cursor-pointer"
          >
            Graphviz Physical Plan SVG
          </Tabs.Trigger>
          <Tabs.Trigger
            value="graphviz"
            className="px-4 py-2 font-medium text-sm border-b-2 border-transparent text-text-secondary hover:text-text-primary hover:bg-secondary-surface transition-colors data-[state=active]:border-accent data-[state=active]:text-text-primary data-[state=active]:bg-secondary-surface cursor-pointer"
          >
            Graphviz Physical Plan DOT
          </Tabs.Trigger>
        </Tabs.List>

        <Tabs.Content value="table" className="flex-1 overflow-auto">
          <table className="table-auto w-full text-text-primary">
            <thead>
              <tr>
                {result.columns.map(([name, type], index) => (
                  <th
                    key={index}
                    className="px-4 py-2 bg-primary-surface text-left whitespace-nowrap overflow-hidden text-overflow-ellipsis"
                  >
                    {name} ({type})
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {result.rows.map((row, rowIndex) => (
                <tr
                  key={rowIndex}
                  className={
                    rowIndex % 2 === 0
                      ? "bg-secondary-surface"
                      : "bg-primary-surface"
                  }
                >
                  {row.map((cell, cellIndex) => (
                    <td
                      key={cellIndex}
                      className="px-4 py-2 whitespace-nowrap overflow-hidden text-overflow-ellipsis"
                    >
                      {cell.split("\n").map((line, index, arr) => (
                        <React.Fragment key={index}>
                          <span
                            data-tooltip-id="cell-tooltip"
                            data-tooltip-content={line}
                            style={{ whiteSpace: "pre-wrap" }}
                          >
                            {line}
                          </span>
                          {index < arr.length - 1 && <br />}
                        </React.Fragment>
                      ))}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </Tabs.Content>

        <Tabs.Content value="logical" className="flex-1 overflow-auto px-2">
          <SyntaxHighlighter
            language="rust"
            style={oneDark}
            customStyle={{
              border: "1px solid var(--color-border)",
              borderRadius: "6px",
              fontSize: "14px",
              margin: 0,
            }}
          >
            {result.logical_plan}
          </SyntaxHighlighter>
        </Tabs.Content>

        <Tabs.Content value="physical" className="flex-1 overflow-auto px-2">
          <SyntaxHighlighter
            language="rust"
            style={oneDark}
            customStyle={{
              border: "1px solid var(--color-border)",
              borderRadius: "6px",
              fontSize: "14px",
              margin: 0,
            }}
          >
            {result.physical_plan}
          </SyntaxHighlighter>
        </Tabs.Content>
        <Tabs.Content
          value="graphviz_svg"
          className="flex-1 relative overflow-auto"
          ref={tabContentRef}
        >
          {selectedTab === "graphviz_svg" && (
            <button
              onClick={toggleFullscreen}
              className="z-10 px-3 py-1.5 bg-secondary-surface hover:bg-primary-surface text-text-primary rounded-md text-sm font-medium shadow-md transition-all border border-border"
              style={{
                position: 'fixed',
                top: `${buttonPosition.top}px`,
                right: `${buttonPosition.right}px`
              }}
            >
              {isFullscreen ? "Exit Fullscreen" : "Fullscreen"}
            </button>
          )}
          <div className="px-2">
            <div dangerouslySetInnerHTML={{ __html: result.graphviz_svg }} />
          </div>
          {isFullscreen && (
            <div className="fixed inset-0 z-50 bg-primary-surface flex flex-col">
              <div className="flex justify-between items-center p-4 border-b border-border">
                <h2 className="text-lg font-semibold text-text-primary">
                  Graphviz Physical Plan
                </h2>
                <button
                  onClick={toggleFullscreen}
                  className="px-3 py-1.5 bg-accent hover:bg-accent/90 text-white rounded-md text-sm font-medium shadow-md transition-all"
                >
                  Exit Fullscreen
                </button>
              </div>
              <div className="flex-1 overflow-auto p-4" ref={fullscreenContentRef}>
                <div className="flex justify-center min-w-fit">
                  <div dangerouslySetInnerHTML={{ __html: result.graphviz_svg }} />
                </div>
              </div>
            </div>
          )}
        </Tabs.Content>
        <Tabs.Content value="graphviz" className="flex-1 overflow-auto px-2">
          <span
            className="text-text-secondary"
            style={{ whiteSpace: "pre-wrap" }}
          >
            {result.graphviz}
          </span>
        </Tabs.Content>
      </Tabs.Root>
    );
  },
);
