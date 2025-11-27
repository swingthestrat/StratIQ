import React, { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import { ArrowUpDown, ArrowUp, ArrowDown, Check, X, Settings, GripVertical } from 'lucide-react';

// --- Configuration & Constants ---

const FILTER_GROUPS = {
  UNIVERSE: {
    title: 'UNIVERSE',
    type: 'multi',
    options: ['SPY', 'QQQ', 'DIA', 'IWM', 'SECTORS', 'IPO', 'MAJOR ETFS', 'EQUAL WEIGHT', 'ALL'],
    default: ['ALL']
  },
  FILTERS: {
    title: 'FILTERS',
    type: 'single',
    options: ['LIQUID LEADERS', 'STRONG RS', 'WEAK RS', 'NONE'],
    default: ['NONE']
  },
  ACTIONABLE_SETUPS: {
    title: 'ACTIONABLE SETUPS',
    type: 'multi',
    options: ['2dG', '2uR', 'HAMMER', 'SHOOTER', 'INSIDE', 'ALL'],
    default: ['ALL']
  },
  IN_FORCE: {
    title: 'IN FORCE',
    type: 'multi',
    options: ['1-2u', '1-2d', '2d-2u', '2u-2d', '3-2u', '3-2d', 'Bullish', 'Bearish', 'HTF In-Force', 'None'],
    default: ['HTF In-Force']
  },
  FTFC: {
    title: 'FTFC',
    type: 'single',
    options: ['BULLISH', 'BEARISH', 'TTO', 'NO FTFC'],
    default: ['BULLISH']
  },
  TIMEFRAME: {
    title: 'Timeframe',
    type: 'multi',
    options: ['1D', '2D', '3D', '5D', '1W', '2W', '3W', '1M', '1Q', '1Y'],
    default: ['1M']
  }
};

const DEFAULT_COLUMNS = [
  { key: 'ticker', label: 'Ticker', width: 'w-20' },
  { key: 'setup', label: 'Setup', width: 'w-32' },
  { key: 'adr', label: 'ADR%', width: 'w-20' },
  { key: 'price', label: 'Price', width: 'w-24' },
  { key: 'industry', label: 'Industry', width: 'w-32' },
  { key: 'prevCond2', label: 'Prev (2)', width: 'w-20' },
  { key: 'prevCond1', label: 'Prev (1)', width: 'w-20' },
  { key: 'currCond', label: 'Curr', width: 'w-20' },
  { key: 'gap', label: 'Gap%', width: 'w-24' },
  { key: 'changeFromOpen', label: 'Chg Open', width: 'w-24' },
  { key: 'wtd', label: 'WTD', width: 'w-24' },
  { key: 'mtd', label: 'MTD', width: 'w-24' },
  { key: 'qtd', label: 'QTD', width: 'w-24' },
  { key: 'ytd', label: 'YTD', width: 'w-24' },
  { key: 'rs_1d', label: 'RS 1D', width: 'w-20' },
  { key: 'rs_1w', label: 'RS 1W', width: 'w-20' },
  { key: 'rs_1m', label: 'RS 1M', width: 'w-20' },
  { key: 'rs_3m', label: 'RS 3M', width: 'w-20' },
  { key: 'timeframe', label: 'Timeframe', width: 'w-24' },
];

// --- Error Boundary ---
class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, errorInfo) {
    console.error("ErrorBoundary caught an error", error, errorInfo);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="p-6 text-red-600">
          <h1 className="text-2xl font-bold mb-2">Something went wrong</h1>
          <pre className="bg-red-50 p-4 rounded border border-red-200">{this.state.error.toString()}</pre>
        </div>
      );
    }

    return this.props.children;
  }
}

// --- Helper Components ---

function DebouncedInput({ value: initialValue, onChange, debounce = 300, ...props }) {
  const [value, setValue] = useState(initialValue);

  useEffect(() => {
    setValue(initialValue);
  }, [initialValue]);

  useEffect(() => {
    const timeout = setTimeout(() => {
      onChange(value);
    }, debounce);
    return () => clearTimeout(timeout);
  }, [value, debounce, onChange]);

  return (
    <input {...props} value={value} onChange={e => setValue(e.target.value)} />
  );
}

// --- Main Component ---
export default function StratScanner() {
  return (
    <ErrorBoundary>
      <StratScannerInner />
    </ErrorBoundary>
  );
}

function StratScannerInner() {
  const [filters, setFilters] = useState(() => {
    const initial = {};
    Object.keys(FILTER_GROUPS).forEach(key => {
      initial[key] = FILTER_GROUPS[key].default;
    });
    return initial;
  });

  const [sortConfig, setSortConfig] = useState({ key: 'gap', direction: 'descending' });
  const [columnFilters, setColumnFilters] = useState({});

  // Column order and visibility state with persistence
  const [columnOrder, setColumnOrder] = useState(() => {
    try {
      const saved = localStorage.getItem('strat-scanner-column-order');
      return saved ? JSON.parse(saved) : DEFAULT_COLUMNS.map(c => c.key);
    } catch {
      return DEFAULT_COLUMNS.map(c => c.key);
    }
  });

  const [visibleColumns, setVisibleColumns] = useState(() => {
    try {
      const saved = localStorage.getItem('strat-scanner-visible-columns');
      return saved ? new Set(JSON.parse(saved)) : new Set(DEFAULT_COLUMNS.map(c => c.key));
    } catch {
      return new Set(DEFAULT_COLUMNS.map(c => c.key));
    }
  });

  const [showColumnSelector, setShowColumnSelector] = useState(false);
  const [draggedColumn, setDraggedColumn] = useState(null);

  // Save to localStorage whenever order or visibility changes
  useEffect(() => {
    try {
      localStorage.setItem('strat-scanner-column-order', JSON.stringify(columnOrder));
    } catch (e) {
      console.error('Failed to save column order:', e);
    }
  }, [columnOrder]);

  useEffect(() => {
    try {
      localStorage.setItem('strat-scanner-visible-columns', JSON.stringify([...visibleColumns]));
    } catch (e) {
      console.error('Failed to save visible columns:', e);
    }
  }, [visibleColumns]);

  const toggleColumn = (key) => {
    setVisibleColumns(prev => {
      const newSet = new Set(prev);
      if (newSet.has(key)) {
        newSet.delete(key);
      } else {
        newSet.add(key);
      }
      return newSet;
    });
  };

  const resetColumns = () => {
    setColumnOrder(DEFAULT_COLUMNS.map(c => c.key));
    setVisibleColumns(new Set(DEFAULT_COLUMNS.map(c => c.key)));
    localStorage.removeItem('strat-scanner-column-order');
    localStorage.removeItem('strat-scanner-visible-columns');
  };

  // Drag and drop handlers
  const handleDragStart = (e, columnKey) => {
    setDraggedColumn(columnKey);
    e.dataTransfer.effectAllowed = 'move';
  };

  const handleDragOver = (e) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'move';
  };

  const handleDrop = (e, targetColumnKey) => {
    e.preventDefault();

    if (!draggedColumn || draggedColumn === targetColumnKey) {
      setDraggedColumn(null);
      return;
    }

    setColumnOrder(prev => {
      const newOrder = [...prev];
      const draggedIndex = newOrder.indexOf(draggedColumn);
      const targetIndex = newOrder.indexOf(targetColumnKey);

      // Remove dragged item
      newOrder.splice(draggedIndex, 1);
      // Insert at target position
      newOrder.splice(targetIndex, 0, draggedColumn);

      return newOrder;
    });

    setDraggedColumn(null);
  };

  const handleDragEnd = () => {
    setDraggedColumn(null);
  };

  const [lastUpdated, setLastUpdated] = useState(new Date());
  const [timeAgo, setTimeAgo] = useState('just now');

  useEffect(() => {
    const interval = setInterval(() => {
      const diff = Math.floor((new Date() - lastUpdated) / 60000);
      setTimeAgo(diff < 1 ? 'just now' : `${diff} mins ago`);
    }, 60000);
    return () => clearInterval(interval);
  }, [lastUpdated]);

  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);

  // Fetch Data from API
  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      try {
        const params = new URLSearchParams();
        if (filters.UNIVERSE) params.append('universe', filters.UNIVERSE.join(','));
        if (filters.FILTERS && filters.FILTERS[0] !== 'NONE') params.append('filters', filters.FILTERS[0]);
        if (filters.ACTIONABLE_SETUPS) params.append('setups', filters.ACTIONABLE_SETUPS.join(','));
        if (filters.IN_FORCE) params.append('in_force', filters.IN_FORCE.join(','));
        if (filters.FTFC) params.append('ftfc', filters.FTFC.join(','));
        if (filters.TIMEFRAME) params.append('timeframe', filters.TIMEFRAME.join(','));

        // Use relative path /api/alerts - handled by Vite proxy in dev and Vercel rewrites in prod
        const response = await fetch(`/api/alerts?${params.toString()}`);
        const result = await response.json();

        if (Array.isArray(result)) {
          setData(result);
          setLastUpdated(new Date());
          setTimeAgo('just now');
        } else {
          console.error("API Error:", result);
          setData([]);
        }
      } catch (error) {
        console.error("Failed to fetch alerts:", error);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [filters]);

  const toggleFilter = (groupKey, option) => {
    setFilters(prev => {
      const groupConfig = FILTER_GROUPS[groupKey];
      const currentSelection = prev[groupKey];

      if (groupConfig.type === 'single') {
        return { ...prev, [groupKey]: [option] };
      }

      if (option === 'ALL' || option === 'NONE') {
        return { ...prev, [groupKey]: [option] };
      }

      let newSelection = currentSelection.filter(item => item !== 'ALL' && item !== 'NONE');

      if (newSelection.includes(option)) {
        newSelection = newSelection.filter(item => item !== option);
      } else {
        newSelection = [...newSelection, option];
      }

      if (newSelection.length === 0 && groupKey === 'UNIVERSE') {
        return { ...prev, [groupKey]: ['ALL'] };
      }

      return { ...prev, [groupKey]: newSelection };
    });
  };

  const requestSort = (key) => {
    let direction = 'ascending';
    if (sortConfig.key === key && sortConfig.direction === 'ascending') {
      direction = 'descending';
    }
    setSortConfig({ key, direction });
  };

  const handleColumnFilterChange = useCallback((key, value) => {
    setColumnFilters(prev => ({
      ...prev,
      [key]: value
    }));
  }, []);

  const processedData = useMemo(() => {
    let filteredData = [...data];

    Object.keys(columnFilters).forEach(key => {
      const filterValue = columnFilters[key].toLowerCase();
      if (filterValue) {
        filteredData = filteredData.filter(row => {
          const cellValueStr = String(row[key] || '').toLowerCase();
          const parseCellNum = (val) => {
            if (!val) return NaN;
            return parseFloat(val.replace(/[%$,]/g, ''));
          };

          if (filterValue.startsWith('>')) {
            const numVal = parseFloat(filterValue.substring(1));
            const cellNum = parseCellNum(cellValueStr);
            if (!isNaN(numVal) && !isNaN(cellNum)) {
              return cellNum > numVal;
            }
          } else if (filterValue.startsWith('<')) {
            const numVal = parseFloat(filterValue.substring(1));
            const cellNum = parseCellNum(cellValueStr);
            if (!isNaN(numVal) && !isNaN(cellNum)) {
              return cellNum < numVal;
            }
          }

          return cellValueStr.includes(filterValue);
        });
      }
    });

    if (sortConfig.key) {
      filteredData.sort((a, b) => {
        let aValue = a[sortConfig.key];
        let bValue = b[sortConfig.key];

        const parseVal = (v) => {
          if (typeof v === 'string' && v.includes('%')) return parseFloat(v.replace('%', ''));
          if (!isNaN(parseFloat(v))) return parseFloat(v);
          return v;
        }

        aValue = parseVal(aValue);
        bValue = parseVal(bValue);

        if (aValue < bValue) {
          return sortConfig.direction === 'ascending' ? -1 : 1;
        }
        if (aValue > bValue) {
          return sortConfig.direction === 'ascending' ? 1 : -1;
        }
        return 0;
      });
    }
    return filteredData;
  }, [data, sortConfig, columnFilters]);

  // Create ordered columns based on saved order
  const orderedColumns = useMemo(() => {
    const columnMap = new Map(DEFAULT_COLUMNS.map(col => [col.key, col]));
    return columnOrder
      .map(key => columnMap.get(key))
      .filter(col => col && visibleColumns.has(col.key));
  }, [columnOrder, visibleColumns]);

  const getConditionColor = (cond) => {
    switch (cond) {
      case '2U': return 'bg-green-100 text-green-700';
      case '2D': return 'bg-red-100 text-red-700';
      case '3': return 'bg-yellow-100 text-yellow-800';
      case '1': return 'bg-blue-100 text-blue-800';
      case '2u': return 'bg-green-100 text-green-700';
      case '2uR': return 'bg-green-100 text-red-700';
      case '2d': return 'bg-red-100 text-red-700';
      case '2dG': return 'bg-red-100 text-green-700';
      case '3u': return 'bg-green-100 text-green-700 border-2 border-yellow-400';
      case '3d': return 'bg-red-100 text-red-700 border-2 border-yellow-400';
      default: return 'bg-gray-100 text-gray-600';
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 text-gray-900 font-sans">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-0 z-10">
        <div className="w-full px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="bg-blue-600 text-white p-1.5 rounded-lg">
              <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M13 10V3L4 14h7v7l9-11h-7z" /></svg>
            </div>
            <h1 className="text-xl font-bold tracking-tight text-gray-900">Swing TheSTRAT</h1>
          </div>
          <div className="flex items-center gap-4">
            <div className="text-xs font-medium text-green-600 bg-green-50 px-2 py-1 rounded-full border border-green-100 flex items-center gap-1">
              <span className="w-1.5 h-1.5 rounded-full bg-green-500 animate-pulse"></span>
              Market Status: OPEN
            </div>
            {Object.values(columnFilters).some(v => v) && (
              <button
                onClick={() => setColumnFilters({})}
                className="text-xs font-medium text-red-600 bg-red-50 px-3 py-1 rounded-full border border-red-200 hover:bg-red-100 transition-colors flex items-center gap-1.5"
              >
                <X size={12} />
                Clear Filters
              </button>
            )}
          </div>
        </div>
      </header>

      <main className="w-full px-4 sm:px-6 lg:px-8 py-6 space-y-6">

        {/* Filters Section */}
        <div className="bg-white p-4 rounded-xl shadow-sm border border-gray-200 overflow-x-auto">
          <div className="flex flex-nowrap gap-8 min-w-max">
            {Object.entries(FILTER_GROUPS).map(([groupKey, group]) => (
              <div key={groupKey} className="flex flex-col gap-2">
                <h3 className="text-xs font-bold text-gray-400 uppercase tracking-wider mb-1 border-b border-gray-100 pb-1">
                  {groupKey}
                </h3>
                <div className="flex flex-col gap-1.5">
                  <div className={`grid ${group.options.length > 5 ? 'grid-cols-2' : 'grid-cols-1'} gap-x-2 gap-y-1`}>
                    {group.options.map(option => {
                      const isSelected = filters[groupKey]?.includes(option);
                      return (
                        <button
                          key={option}
                          onClick={() => toggleFilter(groupKey, option)}
                          className={`
                            px-3 py-1 rounded text-xs font-medium border transition-all duration-200 text-left flex items-center justify-between
                            ${isSelected
                              ? 'bg-green-100 border-green-300 text-green-800 shadow-sm'
                              : 'bg-white border-slate-200 text-slate-600 hover:border-slate-300 hover:bg-slate-50'}
                          `}
                        >
                          {option}
                          {isSelected && <Check className="h-3 w-3 ml-2 opacity-50" />}
                        </button>
                      );
                    })}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Table Section */}
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
          <div className="px-5 py-4 border-b border-gray-200 flex justify-between items-center bg-gray-50/50">
            <div className="flex items-center gap-2">
              <h2 className="text-sm font-semibold text-gray-700">Showing {processedData.length} entries</h2>
            </div>
            <div className="flex items-center gap-2 relative">
              <span className="text-xs text-gray-400">Last updated {timeAgo}</span>

              <button
                onClick={resetColumns}
                className="flex items-center gap-1 px-3 py-1.5 bg-white border border-gray-300 rounded-md text-xs font-medium text-gray-700 hover:bg-gray-50 shadow-sm"
              >
                Reset Layout
              </button>

              <div className="relative">
                <button
                  onClick={() => setShowColumnSelector(!showColumnSelector)}
                  className="flex items-center gap-1 px-3 py-1.5 bg-white border border-gray-300 rounded-md text-xs font-medium text-gray-700 hover:bg-gray-50 shadow-sm"
                >
                  <Settings size={14} />
                  Columns
                </button>

                {showColumnSelector && (
                  <div className="absolute right-0 mt-2 w-48 bg-white rounded-md shadow-lg border border-gray-200 z-50 p-2 max-h-64 overflow-y-auto">
                    <div className="text-xs font-semibold text-gray-500 mb-2 px-2">Visible Columns</div>
                    {DEFAULT_COLUMNS.map(col => (
                      <label key={col.key} className="flex items-center gap-2 px-2 py-1.5 hover:bg-gray-50 rounded cursor-pointer">
                        <input
                          type="checkbox"
                          checked={visibleColumns.has(col.key)}
                          onChange={() => toggleColumn(col.key)}
                          className="rounded border-gray-300 text-blue-600 focus:ring-blue-500 h-3 w-3"
                        />
                        <span className="text-xs text-gray-700">{col.label}</span>
                      </label>
                    ))}
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* Scrollable Table Container */}
          <div className="overflow-x-auto">
            <div className="inline-block min-w-full align-middle">
              <table className="min-w-full divide-y divide-gray-200">
                {/* Header */}
                <thead className="bg-gray-50 sticky top-0">
                  <tr>
                    {orderedColumns.map(col => (
                      <th
                        key={col.key}
                        className={`px-3 py-3 text-left ${col.width} ${draggedColumn === col.key ? 'opacity-50' : ''}`}
                        draggable
                        onDragStart={(e) => handleDragStart(e, col.key)}
                        onDragOver={handleDragOver}
                        onDrop={(e) => handleDrop(e, col.key)}
                        onDragEnd={handleDragEnd}
                      >
                        <div className="flex items-center gap-1">
                          <GripVertical className="h-4 w-4 text-gray-400 cursor-move" />
                          <div
                            className="flex items-center gap-1 cursor-pointer hover:text-gray-700 text-xs font-semibold text-gray-500 uppercase tracking-wider flex-1"
                            onClick={() => requestSort(col.key)}
                          >
                            {col.label}
                            {sortConfig.key === col.key && (
                              <span className="text-gray-900">{sortConfig.direction === 'ascending' ? '↑' : '↓'}</span>
                            )}
                          </div>
                        </div>
                        <div className="mt-2">
                          <DebouncedInput
                            type="text"
                            placeholder={`Filter...`}
                            className="w-full px-2 py-1 text-xs border border-gray-300 rounded focus:outline-none focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
                            value={columnFilters[col.key] || ''}
                            onChange={(value) => handleColumnFilterChange(col.key, value)}
                          />
                        </div>
                      </th>
                    ))}
                  </tr>
                </thead>

                {/* Body */}
                <tbody className="bg-white divide-y divide-gray-100">
                  {loading ? (
                    <tr>
                      <td colSpan={orderedColumns.length} className="px-4 py-8 text-center text-gray-500">
                        <div className="flex justify-center items-center gap-2">
                          <div className="w-4 h-4 border-2 border-blue-600 border-t-transparent rounded-full animate-spin"></div>
                          Loading alerts...
                        </div>
                      </td>
                    </tr>
                  ) : processedData.length === 0 ? (
                    <tr>
                      <td colSpan={orderedColumns.length} className="px-4 py-8 text-center text-gray-500">
                        No alerts found matching the criteria.
                      </td>
                    </tr>
                  ) : (
                    processedData.map((row, idx) => (
                      <tr key={idx} className="hover:bg-blue-50/50 transition-colors">
                        {orderedColumns.map(col => {
                          let val = row[col.key];
                          let content = val;

                          if (['gap', 'changeFromOpen', 'wtd', 'mtd', 'qtd', 'ytd', 'perf_3m'].includes(col.key)) {
                            const numVal = parseFloat(String(val).replace('%', ''));
                            let colorClass = 'text-gray-900';
                            if (numVal > 0) colorClass = 'text-green-600 font-medium';
                            if (numVal < 0) colorClass = 'text-red-600 font-medium';
                            content = <span className={colorClass}>{val}</span>;
                          }

                          if (['prevCond2', 'prevCond1', 'currCond'].includes(col.key)) {
                            content = <span className={`px-1.5 py-0.5 rounded text-xs ${getConditionColor(val)}`}>{val}</span>;
                          }

                          if (['rs_1d', 'rs_1w', 'rs_1m', 'rs_3m'].includes(col.key)) {
                            content = String(val).replace('%', '');
                          }

                          return (
                            <td key={col.key} className={`px-3 py-3 text-sm text-gray-600 whitespace-nowrap ${col.width}`}>
                              {content}
                            </td>
                          );
                        })}
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
