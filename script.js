const animatedMetrics = [...document.querySelectorAll("[data-count]")];
const artifactButtons = [...document.querySelectorAll(".artifact-button")];
const artifactPanels = [...document.querySelectorAll(".artifact-panel")];

function animateCounter(element) {
  const target = Number(element.dataset.count);
  const duration = 1200;
  const startTime = performance.now();

  function tick(now) {
    const progress = Math.min((now - startTime) / duration, 1);
    const eased = 1 - Math.pow(1 - progress, 3);
    element.textContent = Math.round(target * eased);
    if (progress < 1) {
      requestAnimationFrame(tick);
    }
  }

  requestAnimationFrame(tick);
}

function activateArtifact(targetId) {
  artifactButtons.forEach((button) => {
    button.classList.toggle("active", button.dataset.target === targetId);
  });

  artifactPanels.forEach((panel) => {
    panel.classList.toggle("active", panel.id === targetId);
  });
}

document.querySelectorAll("[data-scroll]").forEach((button) => {
  button.addEventListener("click", () => {
    document.querySelector(button.dataset.scroll)?.scrollIntoView({
      behavior: "smooth",
      block: "start",
    });
  });
});

artifactButtons.forEach((button) => {
  button.addEventListener("click", () => activateArtifact(button.dataset.target));
});

animatedMetrics.forEach(animateCounter);
