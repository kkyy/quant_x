import { NavLink } from "react-router-dom";

const NAV_ITEMS = [
  { to: "/", label: "Dashboard", icon: "◉" },
  { to: "/data", label: "Data", icon: "◈" },
  { to: "/models", label: "Models", icon: "◆" },
  { to: "/backtest", label: "Backtest", icon: "◇" },
  { to: "/signals", label: "Signals", icon: "▸" },
  { to: "/factors", label: "Factors", icon: "⋄" },
  { to: "/config", label: "Config", icon: "⚙" },
  { to: "/system", label: "System", icon: "⊙" },
];

export function Sidebar() {
  return (
    <aside className="w-56 border-r border-gray-200 bg-gray-50 h-screen sticky top-0 flex flex-col">
      <div className="p-4 border-b border-gray-200">
        <h1 className="text-lg font-bold">quant_ex</h1>
        <p className="text-xs text-gray-500">Dashboard</p>
      </div>
      <nav className="flex-1 p-2">
        {NAV_ITEMS.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.to === "/"}
            className={({ isActive }) =>
              `flex items-center gap-2 px-3 py-2 rounded-md text-sm transition-colors ${
                isActive
                  ? "bg-gray-900 text-white"
                  : "text-gray-700 hover:bg-gray-200"
              }`
            }
          >
            <span>{item.icon}</span>
            <span>{item.label}</span>
          </NavLink>
        ))}
      </nav>
    </aside>
  );
}
