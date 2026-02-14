// MediaCanon client-side JS

// Season filter
document.querySelectorAll('.season-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        const season = btn.dataset.season;

        // Update active button
        document.querySelectorAll('.season-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');

        // Filter seasons
        document.querySelectorAll('.season').forEach(s => {
            if (season === 'all' || s.dataset.seasonNum === season) {
                s.style.display = '';
            } else {
                s.style.display = 'none';
            }
        });
    });
});
