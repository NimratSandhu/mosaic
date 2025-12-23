// Modern navigation hover effects
document.addEventListener('DOMContentLoaded', function() {
    // Add hover effects to navigation links
    const navLinks = document.querySelectorAll('.nav-link');
    navLinks.forEach(link => {
        link.addEventListener('mouseenter', function() {
            this.style.backgroundColor = '#334155';
            this.style.color = '#f1f5f9';
        });
        link.addEventListener('mouseleave', function() {
            this.style.backgroundColor = 'transparent';
            this.style.color = '#cbd5e1';
        });
    });
});

