import { useTranslation } from 'react-i18next';

export function LanguageToggle() {
  const { i18n } = useTranslation();

  const toggle = (lang: string) => {
    i18n.changeLanguage(lang);
    localStorage.setItem('lang', lang);
  };

  return (
    <div className="flex gap-1 p-3 border-t border-zinc-800">
      {(['zh', 'en'] as const).map((lang) => (
        <button
          key={lang}
          onClick={() => toggle(lang)}
          className={`flex-1 py-1.5 text-xs font-medium rounded transition-colors ${
            i18n.language === lang
              ? 'bg-amber-500 text-zinc-900'
              : 'text-zinc-500 hover:text-zinc-300 hover:bg-zinc-800'
          }`}
        >
          {lang === 'zh' ? '中文' : 'EN'}
        </button>
      ))}
    </div>
  );
}
