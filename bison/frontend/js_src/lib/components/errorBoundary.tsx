/*
 *
 * Error Boundary for React Components. Catches exceptions and provides a
 * stack trace
 *
 *
 */

import React from 'react';

import type { RA } from '../config';
import type { Component } from '../config';
import commonText from '../localization/common';

type ErrorBoundaryState =
  | {
      readonly hasError: false;
    }
  | {
      readonly hasError: true;
      readonly error: { toString: () => string };
      readonly errorInfo: { componentStack: string };
    };

export default class ErrorBoundary extends React.Component<
  { readonly children: Component | null },
  ErrorBoundaryState
> {
  public state: ErrorBoundaryState = {
    hasError: false,
  };

  public componentDidCatch(
    error: { readonly toString: () => string },
    errorInfo: { readonly componentStack: string }
  ): void {
    this.setState({
      hasError: true,
      error,
      errorInfo,
    });
  }

  public render(): Component | null {
    return this.state.hasError ? (
      <ErrorMessage title={commonText('errorBoundaryErrorHeader')}>
        <p>{commonText('errorBoundaryErrorMessage')}</p>
        <details style={{ whiteSpace: 'pre-wrap' }}>
          {this.state.error?.toString()}
          <br />
          {this.state.errorInfo.componentStack}
        </details>
      </ErrorMessage>
    ) : (
      this.props.children
    );
  }
}

export function ErrorMessage({
  title,
  children,
}: {
  title: string;
  children: Component | RA<Component>;
}): Component {
  return (
    <section className="alert alert-error">
      <h1>{title}</h1>
      {children}
    </section>
  );
}
