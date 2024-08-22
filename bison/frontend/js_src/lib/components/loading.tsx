import React from 'react';

import type { Component } from '../config';

export function Loading(): Component {
  return (
    <div id="loader">
      <div>
        <span />
        <div className="logo-container">
          <div className="logo">
            <img
              src="/static/img/specify_network_square.svg"
              alt="Specify Network"
            />
          </div>
        </div>
        <h1>Extending the Specimen...</h1>
      </div>
    </div>
  );
}
