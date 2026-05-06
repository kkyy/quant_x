import { useRef, useEffect, useCallback, type CSSProperties } from 'react';
import * as echarts from 'echarts/core';
import { CanvasRenderer } from 'echarts/renderers';

// Register renderer
echarts.use([CanvasRenderer]);

// Register chart types
import { LineChart } from 'echarts/charts';
import { BarChart } from 'echarts/charts';
import { CandlestickChart } from 'echarts/charts';
import { HeatmapChart } from 'echarts/charts';
import { ScatterChart } from 'echarts/charts';

echarts.use([LineChart, BarChart, CandlestickChart, HeatmapChart, ScatterChart]);

// Register components
import { TitleComponent } from 'echarts/components';
import { TooltipComponent } from 'echarts/components';
import { LegendComponent } from 'echarts/components';
import { GridComponent } from 'echarts/components';
import { DataZoomComponent } from 'echarts/components';
import { ToolboxComponent } from 'echarts/components';
import { VisualMapComponent } from 'echarts/components';

echarts.use([
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent,
  ToolboxComponent,
  VisualMapComponent,
]);

export type EChartsEventHandlers = Record<
  string,
  (params?: unknown) => void
>;

export interface EChartsWrapperProps {
  /** ECharts option object */
  option: Record<string, unknown>;
  /** Container height in pixels (default 400) */
  height?: number;
  /** Show loading overlay */
  loading?: boolean;
  /** Map of event name -> handler */
  onEvents?: EChartsEventHandlers;
  /** Additional CSS class for the container div */
  className?: string;
}

/** EChartsInstance type alias for echarts v6 */
export type EChartsInstance = ReturnType<typeof echarts.init>;

/**
 * Reusable ECharts React wrapper with tree-shaken echarts imports.
 * Uses dark theme by default.
 */
export function EChartsWrapper({
  option,
  height = 400,
  loading = false,
  onEvents,
  className,
}: EChartsWrapperProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<EChartsInstance | null>(null);

  // Initialize echarts instance
  const initChart = useCallback(() => {
    if (!containerRef.current) return;
    const instance = echarts.init(containerRef.current, 'dark', {
      renderer: 'canvas',
    });
    chartRef.current = instance;
  }, []);

  // Dispose on unmount
  useEffect(() => {
    return () => {
      chartRef.current?.dispose();
      chartRef.current = null;
    };
  }, []);

  // Init chart when container is available
  useEffect(() => {
    if (containerRef.current && !chartRef.current) {
      initChart();
    }
  }, [initChart]);

  // Update option when it changes
  useEffect(() => {
    if (!chartRef.current) return;
    chartRef.current.setOption(option, { notMerge: false, lazyUpdate: true });
  }, [option]);

  // Toggle loading overlay
  useEffect(() => {
    if (!chartRef.current) return;
    if (loading) {
      chartRef.current.showLoading('default', { text: 'Loading...', color: '#4ade80', textColor: '#a1a1aa' });
    } else {
      chartRef.current.hideLoading();
    }
  }, [loading]);

  // Bind / rebind event handlers
  useEffect(() => {
    const instance = chartRef.current;
    if (!instance || !onEvents) return;
    Object.entries(onEvents).forEach(([eventName, handler]) => {
      instance.on(eventName, handler);
    });
    return () => {
      if (!instance) return;
      Object.keys(onEvents).forEach((eventName) => {
        instance.off(eventName);
      });
    };
  }, [onEvents]);

  // Handle window resize
  useEffect(() => {
    const handleResize = () => {
      chartRef.current?.resize();
    };
    window.addEventListener('resize', handleResize);
    return () => {
      window.removeEventListener('resize', handleResize);
    };
  }, []);

  const containerStyle: CSSProperties = {
    height: `${height}px`,
    width: '100%',
  };

  return (
    <div
      ref={containerRef}
      style={containerStyle}
      className={className}
    />
  );
}
