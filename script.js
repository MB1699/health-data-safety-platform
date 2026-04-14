const numberWords = ["One", "Two", "Three", "Four", "Five", "Six"];
const countingEmoji = ["🐥", "🐶", "🐸", "🐞", "🦋", "🍎"];
const storyThemes = ["🍎", "🐥", "⭐", "🧸"];

const countingEmojiRow = document.getElementById("counting-emoji");
const countingChoices = document.getElementById("counting-choices");
const countingFeedback = document.getElementById("counting-feedback");

const matchWord = document.getElementById("match-word");
const matchChoices = document.getElementById("match-choices");
const matchFeedback = document.getElementById("match-feedback");

const storyIcons = document.getElementById("story-icons");
const storyProblem = document.getElementById("story-problem");
const storyChoices = document.getElementById("story-choices");
const storyFeedback = document.getElementById("story-feedback");

const progressBadges = [...document.querySelectorAll(".badge")];
const calculatorScreen = document.getElementById("calculator-screen");
const calculatorFeedback = document.getElementById("calculator-feedback");

let calculatorExpression = "0";

function shuffle(items) {
  return [...items].sort(() => Math.random() - 0.5);
}

function setProgress(index) {
  progressBadges.forEach((badge, badgeIndex) => {
    badge.classList.toggle("active", badgeIndex <= index);
  });
}

function buildChoiceButton(label, onClick) {
  const button = document.createElement("button");
  button.className = "choice-button";
  button.type = "button";
  button.textContent = label;
  button.addEventListener("click", () => onClick(button));
  return button;
}

function resetChoiceStyles(container) {
  container.querySelectorAll(".choice-button").forEach((button) => {
    button.classList.remove("correct", "wrong");
  });
}

function newCountingRound() {
  resetChoiceStyles(countingChoices);
  const answer = Math.floor(Math.random() * 5) + 1;
  const icon = countingEmoji[Math.floor(Math.random() * countingEmoji.length)];
  const options = shuffle([
    answer,
    Math.max(1, answer - 1),
    Math.min(6, answer + 1),
  ]).filter((value, index, array) => array.indexOf(value) === index);

  while (options.length < 3) {
    const randomOption = Math.floor(Math.random() * 6) + 1;
    if (!options.includes(randomOption)) {
      options.push(randomOption);
    }
  }

  countingEmojiRow.innerHTML = "";
  for (let index = 0; index < answer; index += 1) {
    const item = document.createElement("span");
    item.textContent = icon;
    countingEmojiRow.appendChild(item);
  }

  countingChoices.innerHTML = "";
  shuffle(options).forEach((option) => {
    countingChoices.appendChild(buildChoiceButton(String(option), (button) => {
      resetChoiceStyles(countingChoices);
      if (option === answer) {
        button.classList.add("correct");
        countingFeedback.textContent = `Yay! There are ${answer}.`;
        setProgress(1);
      } else {
        button.classList.add("wrong");
        countingFeedback.textContent = "Almost. Count one more time.";
      }
    }));
  });

  countingFeedback.textContent = "Let’s count together.";
}

function newMatchRound() {
  resetChoiceStyles(matchChoices);
  const answer = Math.floor(Math.random() * 6) + 1;
  matchWord.textContent = numberWords[answer - 1];

  const options = shuffle([
    answer,
    Math.max(1, answer - 1),
    Math.min(6, answer + 1),
    ((answer + 2) % 6) + 1,
  ]).filter((value, index, array) => array.indexOf(value) === index).slice(0, 4);

  matchChoices.innerHTML = "";
  shuffle(options).forEach((option) => {
    matchChoices.appendChild(buildChoiceButton(String(option), (button) => {
      resetChoiceStyles(matchChoices);
      if (option === answer) {
        button.classList.add("correct");
        matchFeedback.textContent = `${numberWords[answer - 1]} means ${answer}. Great job!`;
        setProgress(2);
      } else {
        button.classList.add("wrong");
        matchFeedback.textContent = "Nice try. Look for the matching number.";
      }
    }));
  });

  matchFeedback.textContent = "Numbers can be friends.";
}

function newStoryRound() {
  resetChoiceStyles(storyChoices);
  const first = Math.floor(Math.random() * 4) + 1;
  const second = Math.floor(Math.random() * 3) + 1;
  const operation = Math.random() > 0.5 ? "+" : "-";
  const icon = storyThemes[Math.floor(Math.random() * storyThemes.length)];
  const answer = operation === "+" ? first + second : Math.max(1, first - second);
  const actualSecond = operation === "-" ? Math.min(second, first - 1 || 1) : second;
  const result = operation === "+" ? first + second : first - actualSecond;

  storyIcons.innerHTML = "";
  for (let index = 0; index < first; index += 1) {
    const item = document.createElement("span");
    item.textContent = icon;
    storyIcons.appendChild(item);
  }

  storyProblem.textContent = operation === "+"
    ? `${first} + ${second} = ?`
    : `${first} - ${actualSecond} = ?`;

  const options = shuffle([
    result,
    Math.max(0, result - 1),
    result + 1,
  ]).filter((value, index, array) => array.indexOf(value) === index);

  while (options.length < 3) {
    const randomOption = Math.floor(Math.random() * 6);
    if (!options.includes(randomOption)) {
      options.push(randomOption);
    }
  }

  storyChoices.innerHTML = "";
  shuffle(options).forEach((option) => {
    storyChoices.appendChild(buildChoiceButton(String(option), (button) => {
      resetChoiceStyles(storyChoices);
      if (option === result) {
        button.classList.add("correct");
        storyFeedback.textContent = `Wonderful! The answer is ${result}.`;
        setProgress(3);
      } else {
        button.classList.add("wrong");
        storyFeedback.textContent = "Good try. Use the pictures to help.";
      }
    }));
  });

  storyFeedback.textContent = "Stories make maths fun.";
}

function formatForScreen(value) {
  return value.replace(/\*/g, "×").replace(/\//g, "÷");
}

function sanitizeExpression(expression) {
  return expression.replace(/×/g, "*").replace(/÷/g, "/");
}

function updateCalculatorScreen() {
  calculatorScreen.textContent = formatForScreen(calculatorExpression);
}

function handleCalculatorInput(button) {
  const value = button.dataset.value;
  const action = button.dataset.action;

  if (action === "clear") {
    calculatorExpression = "0";
    calculatorFeedback.textContent = "All clear. Try a new sum.";
    updateCalculatorScreen();
    return;
  }

  if (action === "back") {
    calculatorExpression = calculatorExpression.length > 1 ? calculatorExpression.slice(0, -1) : "0";
    calculatorFeedback.textContent = "One step back.";
    updateCalculatorScreen();
    return;
  }

  if (action === "equals") {
    try {
      const safeExpression = sanitizeExpression(calculatorExpression);
      if (!/^[0-9+\-*/. ]+$/.test(safeExpression)) {
        throw new Error("Bad expression");
      }

      const result = Function(`"use strict"; return (${safeExpression})`)();
      if (!Number.isFinite(result)) {
        throw new Error("Bad result");
      }

      calculatorExpression = String(Number(result.toFixed(2)));
      calculatorFeedback.textContent = `Awesome! The answer is ${calculatorExpression}.`;
      setProgress(3);
      updateCalculatorScreen();
    } catch (error) {
      calculatorFeedback.textContent = "Oops. Try a simple sum like 3 + 2.";
    }
    return;
  }

  if (!value) {
    return;
  }

  const isOperator = ["+", "-", "×", "÷"].includes(value);
  const lastChar = calculatorExpression.at(-1);

  if (calculatorExpression === "0" && !isOperator) {
    calculatorExpression = value;
  } else if (isOperator && ["+", "-", "×", "÷"].includes(lastChar)) {
    calculatorExpression = `${calculatorExpression.slice(0, -1)}${value}`;
  } else {
    calculatorExpression += value;
  }

  calculatorFeedback.textContent = "Keep going.";
  updateCalculatorScreen();
}

document.querySelectorAll("[data-scroll]").forEach((button) => {
  button.addEventListener("click", () => {
    document.querySelector(button.dataset.scroll)?.scrollIntoView({
      behavior: "smooth",
      block: "start",
    });
  });
});

document.getElementById("new-counting-round").addEventListener("click", newCountingRound);
document.getElementById("new-match-round").addEventListener("click", newMatchRound);
document.getElementById("new-story-round").addEventListener("click", newStoryRound);
document.querySelectorAll(".calc-button").forEach((button) => {
  button.addEventListener("click", () => handleCalculatorInput(button));
});

newCountingRound();
newMatchRound();
newStoryRound();
updateCalculatorScreen();
