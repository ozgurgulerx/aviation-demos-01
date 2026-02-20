export const motionTokens = {
  micro: 0.16,
  state: 0.22,
  panel: 0.32,
  easeOut: [0.16, 1, 0.3, 1] as const,
  easeInOut: [0.4, 0, 0.2, 1] as const,
  spring: {
    type: "spring" as const,
    stiffness: 180,
    damping: 24,
    mass: 0.8,
  },
};

export function fadeUp(reducedMotion: boolean, y = 8) {
  if (reducedMotion) {
    return {
      initial: { opacity: 1, y: 0 },
      animate: { opacity: 1, y: 0 },
      exit: { opacity: 1, y: 0 },
      transition: { duration: 0 },
    };
  }

  return {
    initial: { opacity: 0, y },
    animate: { opacity: 1, y: 0 },
    exit: { opacity: 0, y: -4 },
    transition: {
      duration: motionTokens.state,
      ease: motionTokens.easeOut,
    },
  };
}

export function subtlePulse(enabled: boolean, reducedMotion: boolean) {
  if (!enabled || reducedMotion) {
    return {};
  }

  return {
    boxShadow: [
      "0 0 0 0 rgba(30, 98, 200, 0.00)",
      "0 0 0 6px rgba(30, 98, 200, 0.15)",
      "0 0 0 0 rgba(30, 98, 200, 0.00)",
    ],
    transition: {
      duration: 1.6,
      ease: "easeInOut",
      repeat: Infinity,
    },
  };
}
