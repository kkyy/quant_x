import { NavLink } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { LanguageToggle } from "./LanguageToggle";
import {
  LayoutDashboard,
  Database,
  FlaskConical,
  LineChart,
  Radio,
  Brain,
  Settings,
  Terminal,
} from "lucide-react";

const NAV_ITEMS = [
  { icon: LayoutDashboard, key: "overview", to: "/" },
  { icon: Database, key: "dataExplorer", to: "/data-explorer" },
  { icon: FlaskConical, key: "research", to: "/research" },
  { icon: Brain, key: "models", to: "/models" },
  { icon: LineChart, key: "backtest", to: "/backtest" },
  { icon: Radio, key: "signals", to: "/signals" },
  { icon: Settings, key: "config", to: "/config" },
  { icon: Terminal, key: "system", to: "/system" },
] as const;

export function Sidebar() {
  const { t } = useTranslation();

  return (
    <aside className="w-56 bg-zinc-900 h-screen sticky top-0 flex flex-col border-r border-zinc-800">
      <div className="px-4 py-4 border-b border-zinc-800">
        <h1 className="text-sm font-bold text-zinc-100 tracking-tight">quant_ex</h1>
        <p className="text-xs text-zinc-500 mt-0.5">{t('nav.subtitle')}</p>
      </div>
      <nav className="flex-1 p-2 space-y-0.5 overflow-y-auto">
        {NAV_ITEMS.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.to === "/"}
            className={({ isActive }) =>
              `flex items-center gap-2.5 px-3 py-2 rounded-md text-sm transition-colors ${
                isActive
                  ? "bg-amber-500 text-zinc-900 font-semibold"
                  : "text-zinc-400 hover:text-zinc-200 hover:bg-zinc-800"
              }`
            }
          >
            <item.icon size={16} />
            <span>{t(`nav.${item.key}`)}</span>
          </NavLink>
        ))}
      </nav>
      <LanguageToggle />
    </aside>
  );
}
