import '../../css/header.css';
import '../../css/main.css';

import React from 'react';
import ReactDOM from 'react-dom';

import type { Component } from '../config';
import ErrorBoundary from './errorBoundary';

export function app(Component: () => Component): void {
  ReactDOM.render(
    <React.StrictMode>
      <ErrorBoundary>
        <Component />
      </ErrorBoundary>
    </React.StrictMode>,
    document.getElementById('root')
  );
}
