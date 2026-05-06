interface TabsProps {
  tabs: { key: string; label: string }[];
  activeKey: string;
  onChange: (key: string) => void;
}

export function Tabs({ tabs, activeKey, onChange }: TabsProps) {
  return (
    <div className="flex gap-1 p-1 bg-zinc-800/50 rounded-lg border border-zinc-800 w-fit">
      {tabs.map((tab) => (
        <button
          key={tab.key}
          onClick={() => onChange(tab.key)}
          className={`px-4 py-1.5 text-sm rounded-md font-medium transition-colors ${
            activeKey === tab.key
              ? 'bg-blue-600 text-white'
              : 'text-zinc-400 hover:text-zinc-200 hover:bg-zinc-700'
          }`}
        >
          {tab.label}
        </button>
      ))}
    </div>
  );
}
