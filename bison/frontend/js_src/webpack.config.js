const path = require('path');
const { WebpackManifestPlugin } = require('webpack-manifest-plugin');

module.exports = (_environment, argv) => ({
  module: {
    rules: [
      {
        test: /\.(png|jpg|jpeg|svg)$/,
        type: 'asset',
      },
      {
        test: /\.css$/,
        use: ['style-loader', 'css-loader'],
      },
      {
        test: /\.[jt]sx?$/,
        exclude: /(node_modules)/,
        use: [
          {
            loader: 'babel-loader?+cacheDirectory',
            options: {
              presets: [
                [
                  '@babel/preset-env',
                  {
                    useBuiltIns: 'usage',
                    corejs: '3.15',
                    bugfixes: true,
                    browserslistEnv: argv.mode,
                  },
                ],
                ['@babel/preset-react'],
                ['@babel/preset-typescript'],
              ],
            },
          },
        ],
      },
    ],
  },
  resolve: {
    extensions: ['.ts', '.tsx', '.js'],
    symlinks: false,
  },
  plugins: [
    new WebpackManifestPlugin({
      fileName: 'manifest.json',
    }),
  ],
  devtool: argv.mode === 'development' ? 'eval-source-map' : 'source-map',
  entry: {
    frontend: './lib/frontend/entry.tsx',
    stats: './lib/stats/entry.tsx',
  },
  output: {
    path: path.resolve(__dirname, 'dist'),
    publicPath: '/static/js/',
    filename:
      argv.mode === 'development'
        ? '[name].bundle.js'
        : '[name].[contenthash].bundle.js',
    clean: true,
    libraryExport: 'default',
    environment: {
      arrowFunction: true,
      const: true,
      destructuring: true,
      ...(argv.mode === 'development'
        ? {
            bigIntLiteral: true,
            dynamicImport: true,
            forOf: true,
            module: true,
          }
        : {}),
    },
  },
  watchOptions: {
    ignored: '/node_modules/',
  },
  stats: {
    env: true,
    outputPath: true,
    warnings: true,
    errors: true,
    errorDetails: true,
    errorStack: true,
    moduleTrace: true,
    timings: true,
  },
});
