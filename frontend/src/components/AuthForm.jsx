import { useState } from 'react';
import { Loader, Eye, EyeOff } from 'lucide-react';
import { motion } from 'framer-motion';

const API_BASE = "http://localhost:8000";

const AuthForm = ({ onLogin }) => {
  const [isLogin, setIsLogin] = useState(true);
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    try {
      const formData = new URLSearchParams({ username, password });
      const endpoint = isLogin ? '/token' : '/register';
      const response = await fetch(`${API_BASE}${endpoint}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: formData,
      });

      const data = await response.json();
      if (!response.ok) throw new Error(data.detail || 'Request failed');

      if (isLogin) {
        localStorage.setItem('token', data.access_token);
        localStorage.setItem('username', username);
        onLogin();
      } else {
        setIsLogin(true); // Switch to login view after successful registration
        setError('Registration successful! Please sign in.');
        setPassword('');
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: -20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5 }}
      className="w-full max-w-sm text-gray-300"
    >
      <div className="text-left mb-10">
        <h1 className="text-3xl font-bold text-white">
          {isLogin ? 'Welcome Back 👋' : 'Create an Account'}
        </h1>
        <p className="mt-2 text-gray-400">
          {isLogin
            ? 'Please login with your details here'
            : 'Get started with your free account'}
        </p>
      </div>

      <form onSubmit={handleSubmit} className="space-y-5">
        <div>
          <label className="text-xs font-medium text-gray-400">Username</label>
          <input
            type="text"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            className="w-full mt-1 px-4 py-3 bg-gray-800 border border-gray-700 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none transition"
            placeholder="Enter your username"
            required
          />
        </div>

        <div>
          <label className="text-xs font-medium text-gray-400">Password</label>
          <div className="relative mt-1">
            <input
              type={showPassword ? 'text' : 'password'}
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full px-4 py-3 bg-gray-800 border border-gray-700 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none transition"
              placeholder="Enter your password"
              required
            />
            <button
              type="button"
              onClick={() => setShowPassword(!showPassword)}
              className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-500 hover:text-gray-300"
            >
              {showPassword ? <EyeOff size={20} /> : <Eye size={20} />}
            </button>
          </div>
        </div>

        {error && (
          <p
            className={`text-sm ${
              error.includes('successful')
                ? 'text-green-400'
                : 'text-red-400'
            }`}
          >
            {error}
          </p>
        )}

        <div className="text-right">
          <a
            href="#"
            className="text-xs font-medium text-gray-400 hover:text-blue-400 transition"
          >
            Forgot Password?
          </a>
        </div>

        <motion.button
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
          type="submit"
          disabled={loading}
          className="w-full bg-blue-500 text-white py-3 rounded-lg font-semibold hover:bg-blue-600 transition disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {loading ? (
            <Loader className="animate-spin mx-auto" />
          ) : isLogin ? (
            'Sign In'
          ) : (
            'Create Account'
          )}
        </motion.button>
      </form>

      <div className="text-center mt-8 text-sm">
        <p>
          {isLogin
            ? "Don't have an account?"
            : 'Already have an account?'}
          <button
            onClick={() => {
              setIsLogin(!isLogin);
              setError('');
            }}
            className="font-semibold text-blue-400 hover:underline ml-2"
          >
            {isLogin ? 'Sign Up' : 'Sign In'}
          </button>
        </p>
      </div>
    </motion.div>
  );
};

export default AuthForm;
