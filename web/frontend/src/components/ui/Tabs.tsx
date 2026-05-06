import { motion } from "framer-motion";

interface TabsProps {
  tabs: { key: string; label: string }[];
  activeKey: string;
  onChange: (key: string) => void;
}

export function Tabs({ tabs, activeKey, onChange }: TabsProps) {
  return (
    <div className="flex border-b border-terminal-border">
      {tabs.map((tab) => (
        <button
          key={tab.key}
          onClick={() => onChange(tab.key)}
          className={`relative px-4 py-2 text-xs font-mono font-medium tracking-wide transition-colors ${
            activeKey === tab.key
              ? "text-terminal-green"
              : "text-terminal-text-dim hover:text-terminal-text"
          }`}
        >
          {tab.label}
          {activeKey === tab.key && (
            <motion.div
              layoutId="tab-underline"
              className="absolute bottom-0 left-0 right-0 h-[2px] bg-terminal-green"
              transition={{ type: "spring", stiffness: 500, damping: 35 }}
            />
          )}
        </button>
      ))}
    </div>
  );
}
