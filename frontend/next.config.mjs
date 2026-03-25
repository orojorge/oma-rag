const API_URL = "http://localhost:8000"

/** @type {import('next').NextConfig} */
const nextConfig = {
	devIndicators: false,
	
    async rewrites() {
		return [
			{
				source: '/api/:path*',
				destination: `${API_URL}/:path*`,
			},
		]
	},
};

export default nextConfig;
