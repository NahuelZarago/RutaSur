/** @type {import('next').NextConfig} */
const nextConfig = {
  // Sin compresión: gzip bufferiza las respuestas y rompe el streaming de Server-Sent Events.
  compress: false,
  poweredByHeader: false,
  // El proyecto no incluye ESLint: evita que el build se detenga por no encontrarlo.
  eslint: { ignoreDuringBuilds: true },
  experimental: {
    serverComponentsExternalPackages: ['mongodb'],
  },
};

export default nextConfig;
