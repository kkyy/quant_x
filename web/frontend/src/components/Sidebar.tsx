import { NavLink } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { LanguageToggle } from "./LanguageToggle";

const NAV_ITEMS = [
  { to: "/",          icon: "◉", key: "dashboard" },
  { to: "/data",      icon: "◈", key: "data" },
  { to: "/models",    icon: "◆", key: "models" },
  { to: "/backtest",  icon: "◇", key: "backtest" },
  { to: "/signals",   icon: "▸", key: "signals" },
  { to: "/factors",   icon: "⋄", key: "factors" },
  { to: "/config",    icon: "⚙", key: "config" },
  { to: "/system",    icon: "⊙", key: "system" },
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
            <span className="text-base leading-none">{item.icon}</span>
            <span>{t(`nav.${item.key}`)}</span>
          </NavLink>
        ))}
      </nav>
      <LanguageToggle />
    </aside>
  );
}
