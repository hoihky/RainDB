(function () {
  const toggle = document.getElementById('sidebar-toggle');
  const sidebar = document.getElementById('sidebar');
  const search = document.getElementById('search');
  const SIDEBAR_SCROLL_KEY = 'mdweb-sidebar-scroll';

  if (sidebar) {
    restoreSidebarScroll(sidebar);

    sidebar.addEventListener(
      'scroll',
      () => {
        sessionStorage.setItem(SIDEBAR_SCROLL_KEY, String(sidebar.scrollTop));
      },
      { passive: true }
    );

    sidebar.querySelectorAll('a.nav-link').forEach((link) => {
      link.addEventListener('click', () => {
        sessionStorage.setItem(SIDEBAR_SCROLL_KEY, String(sidebar.scrollTop));
      });
    });

    window.addEventListener('beforeunload', () => {
      sessionStorage.setItem(SIDEBAR_SCROLL_KEY, String(sidebar.scrollTop));
    });
  }

  function restoreSidebarScroll(sidebarEl) {
    const saved = sessionStorage.getItem(SIDEBAR_SCROLL_KEY);
    if (saved === null) return;

    const scrollTop = Number.parseInt(saved, 10);
    if (Number.isNaN(scrollTop)) return;

    const apply = () => {
      sidebarEl.scrollTop = scrollTop;
    };

    apply();
    requestAnimationFrame(apply);
  }

  if (toggle && sidebar) {
    toggle.addEventListener('click', () => {
      sidebar.classList.toggle('is-open');
    });

    document.addEventListener('click', (e) => {
      if (!sidebar.contains(e.target) && e.target !== toggle) {
        sidebar.classList.remove('is-open');
      }
    });
  }

  if (search) {
    search.addEventListener('input', () => {
      const query = search.value.trim().toLowerCase();
      document.querySelectorAll('.nav-item').forEach((item) => {
        const link = item.querySelector(':scope > .nav-link, :scope > .nav-group');
        if (!link) return;
        const text = link.textContent?.toLowerCase() ?? '';
        const match = !query || text.includes(query);
        item.classList.toggle('is-hidden', !match && !item.querySelector('.nav-item:not(.is-hidden)'));
        if (query && match) item.classList.remove('is-hidden');
      });
    });
  }

  document.querySelectorAll('pre code').forEach((block) => {
    if (block.classList.contains('language-mermaid') || block.closest('pre.mermaid')) {
      return;
    }
    if (typeof hljs !== 'undefined') {
      hljs.highlightElement(block);
    }
  });

  prepareMermaidBlocks();

  if (document.querySelector('.mermaid') && typeof mermaid !== 'undefined') {
    mermaid.initialize({
      startOnLoad: false,
      theme: 'base',
      themeVariables: {
        darkMode: true,
        background: '#1e293b',
        primaryColor: '#6366f1',
        primaryTextColor: '#f1f5f9',
        primaryBorderColor: '#818cf8',
        lineColor: '#94a3b8',
        secondaryColor: '#334155',
        tertiaryColor: '#0f172a',
        fontFamily: 'Inter, system-ui, sans-serif',
      },
      securityLevel: 'strict',
    });

    mermaid.run({ querySelector: '.mermaid' }).catch((err) => {
      console.error('Mermaid rendering failed:', err);
    });
  }

  function prepareMermaidBlocks() {
    document.querySelectorAll('pre.mermaid').forEach((pre) => {
      const div = document.createElement('div');
      div.className = 'mermaid';
      div.textContent = pre.textContent.trim();
      pre.replaceWith(div);
    });

    document.querySelectorAll('pre > code.language-mermaid').forEach((code) => {
      const pre = code.parentElement;
      if (!pre) return;
      const div = document.createElement('div');
      div.className = 'mermaid';
      div.textContent = code.textContent.trim();
      pre.replaceWith(div);
    });
  }
})();
