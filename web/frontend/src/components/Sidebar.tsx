import { useState } from "react";
import { NavLink } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { LanguageToggle } from "./LanguageToggle";
import { motion } from "framer-motion";
import {
  LayoutDashboard,
  Database,
  FlaskConical,
  LineChart,
  Radio,
  Brain,
  Bot,
  Settings,
  Terminal,
} from "lucide-react";
import { clsx } from "clsx";

const NAV_ITEMS = [
  { icon: LayoutDashboard, key: "overview", to: "/" },
  { icon: Database, key: "dataExplorer", to: "/data-explorer" },
  { icon: FlaskConical, key: "research", to: "/research" },
  { icon: Brain, key: "models", to: "/models" },
  { icon: LineChart, key: "backtest", to: "/backtest" },
  { icon: Radio, key: "signals", to: "/signals" },
  { icon: Bot, key: "agentRuns", to: "/agents" },
  { icon: Settings, key: "config", to: "/config" },
  { icon: Terminal, key: "system", to: "/system" },
] as const;

export function Sidebar() {
  const { t } = useTranslation();
  const [expanded, setExpanded] = useState(false);

  return (
    <motion.aside
      onMouseEnter={() => setExpanded(true)}
      onMouseLeave={() => setExpanded(false)}
      animate={{ width: expanded ? 200 : 52 }}
      transition={{ duration: 0.15, ease: "easeOut" }}
      className="h-screen sticky top-0 flex flex-col bg-terminal-bg border-r border-terminal-border-dim z-40 overflow-hidden"
    >
      {/* Logo */}
      <div className="flex items-center gap-2 px-3 py-3 border-b border-terminal-border-dim h-12">
        <span className="text-sm font-mono font-bold text-terminal-green tracking-tight shrink-0">
          QX
        </span>
        {expanded && (
          <motion.span
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.1 }}
            className="text-xs font-mono text-terminal-text-dim whitespace-nowrap"
          >
            quant_ex
          </motion.span>
        )}
      </div>

      {/* Nav */}
      <nav className="flex-1 py-1.5 space-y-0.5 overflow-y-auto overflow-x-hidden">
        {NAV_ITEMS.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.to === "/"}
            className={({ isActive }) =>
              clsx(
                "flex items-center gap-2.5 mx-1.5 px-2.5 py-2 rounded-sm text-xs font-mono transition-colors relative",
                isActive
                  ? "text-terminal-green bg-terminal-green-glow"
                  : "text-terminal-text-dim hover:text-terminal-text hover:bg-terminal-raised"
              )
            }
          >
            {({ isActive }) => (
              <>
                {isActive && (
                  <motion.div
                    layoutId="sidebar-indicator"
                    className="absolute left-0 top-1 bottom-1 w-[2px] bg-terminal-green rounded-full"
                    transition={{ type: "spring", stiffness: 500, damping: 35 }}
                  />
                )}
                <item.icon size={15} className="shrink-0" />
                {expanded && (
                  <motion.span
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    transition={{ duration: 0.1 }}
                    className="whitespace-nowrap"
                  >
                    {t(`nav.${item.key}`)}
                  </motion.span>
                )}
              </>
            )}
          </NavLink>
        ))}
      </nav>

      {/* Language */}
      <LanguageToggle />
    </motion.aside>
  );
}
