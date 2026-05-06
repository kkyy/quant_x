import { Outlet, useLocation } from "react-router-dom";
import { Sidebar } from "./Sidebar";
import { ToastContainer } from "./ui/Toast";
import { motion, AnimatePresence } from "framer-motion";

export function Layout() {
  const location = useLocation();

  return (
    <div className="flex min-h-screen bg-terminal-bg">
      <Sidebar />
      <main className="flex-1 overflow-auto relative">
        {/* Top accent line */}
        <div className="absolute top-0 left-0 right-0 h-px bg-gradient-to-r from-terminal-green/40 via-terminal-border to-transparent" />
        <div className="p-5">
          <AnimatePresence mode="wait">
            <motion.div
              key={location.pathname}
              initial={{ opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -4 }}
              transition={{ duration: 0.15, ease: "easeOut" }}
            >
              <Outlet />
            </motion.div>
          </AnimatePresence>
        </div>
      </main>
      <ToastContainer />
    </div>
  );
}
