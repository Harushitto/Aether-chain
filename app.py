/* Add this CSS for the marquee animation */
@keyframes marquee {
    from { transform: translateX(100%); }
    to { transform: translateX(-100%); }
}

.ticker-container {
    overflow: hidden;
    white-space: nowrap;
}

.marquee-text {
    animation: marquee 20s linear infinite;
}

<!-- Wrap wisdom text with <span> -->
<span class="marquee-text">Wisdom text goes here</span>
