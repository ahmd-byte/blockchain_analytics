import axios from 'axios';
import { toast } from 'sonner';

// Default to localhost for development if not specified
const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const client = axios.create({
    baseURL: API_BASE_URL,
    headers: {
        'Content-Type': 'application/json',
    },
});

// Response interceptor for unified error handling
client.interceptors.response.use(
    (response) => response,
    (error) => {
        const message = error.response?.data?.detail || error.message || 'An unexpected error occurred';
        console.error('API Error:', message);
        toast.error(`Error: ${message}`);
        return Promise.reject(error);
    }
);

export default client;
