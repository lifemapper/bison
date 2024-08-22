// Format different parts of the response object

import '../../css/response.css';

import React from 'react';

import type { Component, IR, RA } from '../config';

function List({
  children: values,
}: {
  readonly children: RA<unknown>;
}): Component | null {
  // eslint-disable-next-line unicorn/no-null
  if (values.length === 0) return null;
  else if (values.length === 1) return <Value>{values[0]}</Value>;
  else
    return (
      <Dict listOfValues>
        {Object.fromEntries(
          values.map((value, index) => [`[${index + 1}]`, value])
        )}
      </Dict>
    );
}

function String({
  children: value,
}: {
  readonly children: unknown;
}): Component {
  return (
    <label className="textbox-container">
      <div
        className="textbox"
        role="textbox"
        aria-multiline="true"
        aria-readonly="true"
      >
        {`${value}`}
      </div>
    </label>
  );
}

export function Value({
  children: value,
}: {
  readonly children: unknown;
}): Component | null {
  if (typeof value === 'boolean')
    return (
      <label className="checkbox-with-indicator">
        <input type="checkbox" checked={value} readOnly />
        <span />
      </label>
    );
  else if (Array.isArray(value)) return <List>{value}</List>;
  // eslint-disable-next-line unicorn/no-null
  else if (value === null || typeof value === 'undefined') return null;
  else if (typeof value === 'object')
    return <Dict>{value as IR<unknown>}</Dict>;
  else return <String>{value}</String>;
}

function Line({
  label,
  children: value,
}: {
  readonly label: string;
  readonly children: Component;
}): Component {
  return (
    <div className="field">
      <div className="label">{label}</div>
      <div className="value">{value}</div>
    </div>
  );
}

function Dict({
  children: fields,
  listOfValues = false,
}: {
  readonly children: IR<unknown>;
  readonly listOfValues?: boolean;
}): Component {
  return (
    <div className={`list-of-fields ${listOfValues ? 'list-of-values' : ''}`}>
      {Object.entries(fields).map(([label, value]) => (
        <Line label={label} key={label}>
          <Value>{value}</Value>
        </Line>
      ))}
    </div>
  );
}
