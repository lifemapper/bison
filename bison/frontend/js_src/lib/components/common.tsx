import React from 'react';

import type { Component } from '../config';

export function Section({
  anchor,
  label,
  children,
}: {
  readonly anchor: string;
  readonly label: string;
  readonly children: React.ReactNode;
}): Component | null {
  return (
    <section id={anchor}>
      <h3>{label}</h3>
      {children}
    </section>
  );
}
